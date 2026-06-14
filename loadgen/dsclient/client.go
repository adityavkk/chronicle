// Package dsclient is a deliberately thin Durable Streams wire client.
// It speaks the protocol directly over net/http with no hidden retries,
// no buffering policy, and explicit timing capture — a load generator
// must observe the wire, not a convenience SDK's behavior on top of it.
package dsclient

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/ssewire"
)

// Protocol header names.
const (
	HeaderNextOffset = "Stream-Next-Offset"
	HeaderUpToDate   = "Stream-Up-To-Date"
	HeaderClosed     = "Stream-Closed"
	HeaderCursor     = "Stream-Cursor"
	HeaderTTL        = "Stream-TTL"
	HeaderProdID     = "Producer-Id"
	HeaderProdEpoch  = "Producer-Epoch"
	HeaderProdSeq    = "Producer-Seq"
)

// Client issues Durable Streams requests against one server.
type Client struct {
	hc      *http.Client
	baseURL string
	root    string
}

// New builds a client for baseURL with streams under root. The transport
// is tuned for very high connection counts (SSE tailer populations) and
// HTTP/1.1 (matching browsers/CDNs and both reference servers).
func New(baseURL, root string) *Client {
	transport := &http.Transport{
		Proxy: nil, // never route benchmark traffic through env proxies
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        0, // unlimited
		MaxIdleConnsPerHost: 8192,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		WriteBufferSize:     16 << 10,
		ReadBufferSize:      64 << 10,
	}
	return &Client{
		hc:      &http.Client{Transport: transport},
		baseURL: strings.TrimSuffix(baseURL, "/"),
		root:    root,
	}
}

// StreamURL returns the absolute URL for a stream name.
func (c *Client) StreamURL(name string) string {
	return c.baseURL + c.root + name
}

// Response captures the protocol-relevant parts of an HTTP exchange.
type Response struct {
	Status     int
	NextOffset string
	Cursor     string
	UpToDate   bool
	Closed     bool
	Body       []byte
	TTFB       time.Duration // request start → response headers read
	Total      time.Duration // request start → body fully read
}

func responseFrom(resp *http.Response, start time.Time, readBody bool) (Response, error) {
	r := Response{
		Status:     resp.StatusCode,
		NextOffset: resp.Header.Get(HeaderNextOffset),
		Cursor:     resp.Header.Get(HeaderCursor),
		UpToDate:   resp.Header.Get(HeaderUpToDate) != "",
		Closed:     strings.EqualFold(resp.Header.Get(HeaderClosed), "true"),
		TTFB:       time.Since(start),
	}
	defer resp.Body.Close() //nolint:errcheck // read side; nothing actionable
	if readBody {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return r, fmt.Errorf("read body: %w", err)
		}
		r.Body = body
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	r.Total = time.Since(start)
	return r, nil
}

// Create issues PUT to create a stream.
func (c *Client) Create(ctx context.Context, name, contentType string) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.StreamURL(name), nil)
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", contentType)
	return c.do(req, false)
}

// SubURL returns the absolute URL for a reserved __ds subscription path.
func (c *Client) SubURL(id string) string {
	return c.baseURL + c.root + "__ds/subscriptions/" + id
}

// CreateSubscription issues PUT to create or re-confirm a subscription. body is
// the JSON config: {type, pattern|streams, webhook|wake_stream, lease_ttl_ms}.
func (c *Client) CreateSubscription(ctx context.Context, id string, body []byte) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.SubURL(id), bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, true)
}

// DeleteSubscription tombstones a subscription (idempotent).
func (c *Client) DeleteSubscription(ctx context.Context, id string) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.SubURL(id), nil)
	if err != nil {
		return Response{}, err
	}
	return c.do(req, false)
}

// Delete issues DELETE for a stream.
func (c *Client) Delete(ctx context.Context, name string) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.StreamURL(name), nil)
	if err != nil {
		return Response{}, err
	}
	return c.do(req, false)
}

// Append issues POST with body. extra headers (producer triplet,
// Stream-Closed) are applied verbatim.
func (c *Client) Append(ctx context.Context, name, contentType string, body []byte, extra map[string]string) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.StreamURL(name), bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", contentType)
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	return c.do(req, false)
}

// Close issues a close-only POST (Stream-Closed: true, empty body).
func (c *Client) Close(ctx context.Context, name string) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.StreamURL(name), nil)
	if err != nil {
		return Response{}, err
	}
	req.Header.Set(HeaderClosed, "true")
	return c.do(req, false)
}

// Read issues a catch-up or long-poll GET and reads the full body.
// live is "" (catch-up) or "long-poll"; cursor is echoed when non-empty.
func (c *Client) Read(ctx context.Context, name, offset, live, cursor string) (Response, error) {
	q := url.Values{}
	q.Set("offset", offset)
	if live != "" {
		q.Set("live", live)
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.StreamURL(name)+"?"+q.Encode(), nil)
	if err != nil {
		return Response{}, err
	}
	return c.do(req, true)
}

func (c *Client) do(req *http.Request, readBody bool) (Response, error) {
	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		return Response{}, err
	}
	return responseFrom(resp, start, readBody)
}

// SSEConn is one live SSE connection being tailed.
type SSEConn struct {
	resp    *http.Response
	scanner *bufio.Scanner
	parser  ssewire.Parser
	// ConnectTTFB is request start → response headers.
	ConnectTTFB time.Duration
}

// OpenSSE starts a live SSE read. The returned connection must be
// Closed by the caller. The request context governs the whole stream.
func (c *Client) OpenSSE(ctx context.Context, name, offset, cursor string) (*SSEConn, error) {
	q := url.Values{}
	q.Set("offset", offset)
	q.Set("live", "sse")
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.StreamURL(name)+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	start := time.Now()
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close() //nolint:errcheck // error path cleanup
		return nil, fmt.Errorf("sse: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 8<<20) // single events can carry MB-scale batches
	return &SSEConn{resp: resp, scanner: sc, ConnectTTFB: time.Since(start)}, nil
}

// Next blocks until the next complete SSE event. io.EOF means the server
// cycled the connection (normal: reconnect with the last control offset).
func (s *SSEConn) Next() (ssewire.Event, error) {
	for s.scanner.Scan() {
		line := bytes.TrimSuffix(s.scanner.Bytes(), []byte{'\r'})
		if ev, ok := s.parser.Line(line); ok {
			return ev, nil
		}
	}
	if err := s.scanner.Err(); err != nil {
		return ssewire.Event{}, err
	}
	return ssewire.Event{}, io.EOF
}

// Close terminates the connection.
func (s *SSEConn) Close() error { return s.resp.Body.Close() }
