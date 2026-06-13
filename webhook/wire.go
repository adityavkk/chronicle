package webhook

import (
	"encoding/json"
	"time"
)

// This file defines the wire contract of the reserved subscription APIs
// (PROTOCOL §6–7) as Go DTOs plus pure mappers from domain records to response
// shapes. The field names and JSON tags are part of the conformance contract.

// createRequest is the PUT body that creates or re-confirms a subscription.
type createRequest struct {
	Type    DispatchType `json:"type"`
	Pattern string       `json:"pattern"`
	Streams []string     `json:"streams"`
	Webhook *struct {
		URL string `json:"url"`
	} `json:"webhook"`
	WakeStream  string `json:"wake_stream"`
	LeaseTTLMs  int64  `json:"lease_ttl_ms"`
	Description string `json:"description"`
}

// ParseCreateConfig decodes and structurally validates a PUT body into a Config.
// A non-empty reason means the request is a 400; SSRF validation of the webhook
// URL is the caller's separate step (ClassifyWebhookURL).
func ParseCreateConfig(body []byte) (Config, string) {
	var req createRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return Config{}, "invalid JSON body"
	}
	c := Config{
		Type:        req.Type,
		Pattern:     req.Pattern,
		Streams:     req.Streams,
		WakeStream:  req.WakeStream,
		LeaseTTLMs:  req.LeaseTTLMs,
		Description: req.Description,
	}
	if req.Webhook != nil {
		c.WebhookURL = req.Webhook.URL
	}
	c = NormalizeConfig(c)
	if reason := ValidateConfig(c); reason != "" {
		return Config{}, reason
	}
	return c, ""
}

// SigningView is the webhook signing metadata block (PROTOCOL §6.3). Note alg is
// the lowercase "ed25519" here, distinct from the JWK's "EdDSA".
type SigningView struct {
	Alg     string `json:"alg"`
	Kid     string `json:"kid"`
	JWKSURL string `json:"jwks_url"`
}

// HookView is the webhook object in a subscription response. It never
// includes a shared secret (PROTOCOL §6.2: deliveries are asymmetrically signed).
type HookView struct {
	URL     string       `json:"url"`
	Signing *SigningView `json:"signing,omitempty"`
}

// StreamLinkView is a serialized cursor (PROTOCOL §6.1).
type StreamLinkView struct {
	Path        string   `json:"path"`
	LinkType    LinkType `json:"link_type"`
	AckedOffset string   `json:"acked_offset"`
}

// SubscriptionView is the GET / create response body (PROTOCOL §6.3).
type SubscriptionView struct {
	ID             string           `json:"id"`
	SubscriptionID string           `json:"subscription_id"`
	Type           DispatchType     `json:"type"`
	Pattern        string           `json:"pattern,omitempty"`
	Streams        []StreamLinkView `json:"streams"`
	Webhook        *HookView        `json:"webhook,omitempty"`
	WakeStream     *string          `json:"wake_stream"`
	LeaseTTLMs     int64            `json:"lease_ttl_ms"`
	CreatedAt      string           `json:"created_at"`
	Status         Status           `json:"status"`
	Description    string           `json:"description,omitempty"`
}

// BuildSubscriptionView maps a Subscription to its response shape. signing is
// nil for pull-wake subscriptions (no webhook block).
func BuildSubscriptionView(sub Subscription, signing *SigningView) SubscriptionView {
	v := SubscriptionView{
		ID:             sub.ID,
		SubscriptionID: sub.ID,
		Type:           sub.Config.Type,
		Pattern:        sub.Config.Pattern,
		Streams:        linkViews(sub.Links),
		LeaseTTLMs:     sub.Config.LeaseTTLMs,
		CreatedAt:      sub.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		Status:         sub.Status,
		Description:    sub.Config.Description,
	}
	if sub.Config.Type == DispatchWebhook {
		v.Webhook = &HookView{URL: sub.Config.WebhookURL, Signing: signing}
	}
	if sub.Config.WakeStream != "" {
		ws := sub.Config.WakeStream
		v.WakeStream = &ws
	}
	return v
}

func linkViews(links []StreamLink) []StreamLinkView {
	out := make([]StreamLinkView, 0, len(links))
	for _, l := range links {
		out = append(out, StreamLinkView(l))
	}
	return out
}

// WakeNotification is the signed body POSTed to a webhook receiver (PROTOCOL §7.1).
type WakeNotification struct {
	SubscriptionID string           `json:"subscription_id"`
	WakeID         string           `json:"wake_id"`
	Generation     int64            `json:"generation"`
	Streams        []StreamSnapshot `json:"streams"`
	CallbackURL    string           `json:"callback_url"`
	CallbackToken  string           `json:"callback_token"`
}

// CallbackRequest is the body of a webhook callback or a pull-wake ack
// (PROTOCOL §7.1, §7.2). Done is a pointer to distinguish absent from false.
type CallbackRequest struct {
	WakeID     string `json:"wake_id"`
	Generation int64  `json:"generation"`
	Acks       []Ack  `json:"acks"`
	Done       *bool  `json:"done"`
}

// AckResponse is the success body for a callback or ack (PROTOCOL §7.1).
type AckResponse struct {
	OK       bool `json:"ok"`
	NextWake bool `json:"next_wake"`
}

// ClaimRequest is the pull-wake claim body (PROTOCOL §7.2).
type ClaimRequest struct {
	Worker string `json:"worker"`
}

// ClaimResponse is a successful pull-wake claim (PROTOCOL §7.2).
type ClaimResponse struct {
	WakeID     string           `json:"wake_id"`
	Generation int64            `json:"generation"`
	Token      string           `json:"token"`
	Streams    []StreamSnapshot `json:"streams"`
	LeaseTTLMs int64            `json:"lease_ttl_ms"`
}

// ReleaseRequest is the pull-wake release body (PROTOCOL §7.2).
type ReleaseRequest struct {
	WakeID     string `json:"wake_id"`
	Generation int64  `json:"generation"`
}

// WakeEvent is the event a pull-wake subscription writes to its wake_stream
// (PROTOCOL §7.2).
type WakeEvent struct {
	Type           string `json:"type"`
	SubscriptionID string `json:"subscription_id"`
	Stream         string `json:"stream"`
	Generation     int64  `json:"generation"`
	TS             int64  `json:"ts"`
}

// NewWakeEvent builds a wake event marshaled to JSON for appending to a wake
// stream.
func NewWakeEvent(subID, stream string, generation int64, now time.Time) ([]byte, error) {
	return json.Marshal(WakeEvent{
		Type:           "wake",
		SubscriptionID: subID,
		Stream:         stream,
		Generation:     generation,
		TS:             now.UnixMilli(),
	})
}

// ErrorBody is the standard error envelope (PROTOCOL §6.2, §7).
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail carries the error code plus the optional fields used by specific
// errors (current_holder/generation for ALREADY_CLAIMED).
type ErrorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message,omitempty"`
	CurrentHolder string `json:"current_holder,omitempty"`
	Generation    int64  `json:"generation,omitempty"`
}

// errBody is a small constructor for an ErrorBody with just a code.
func errBody(code string) ErrorBody {
	return ErrorBody{Error: ErrorDetail{Code: code}}
}
