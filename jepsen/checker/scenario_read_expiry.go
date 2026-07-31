package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const readExpiryTTLSeconds = 3

// runReadExpiry exercises the public read boundary whose access semantics are
// implemented atomically in Redis. The sleeps stay deliberately far from the
// strict expiry instant: unit and differential tests own exact-boundary checks,
// while this live gate proves that HTTP method and expiry mode reach the right
// storage behavior through the deployed server.
func runReadExpiry(c config) error {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	ordinary := "expiry/ordinary-" + suffix
	sliding := "expiry/sliding-" + suffix
	headOnly := "expiry/head-only-" + suffix
	absolute := "expiry/absolute-" + suffix
	for _, stream := range []string{ordinary, sliding, headOnly, absolute} {
		defer deleteStream(c.base, stream)
	}

	if err := createExpiryStream(c.base, ordinary, "", ""); err != nil {
		return err
	}
	if err := createExpiryStream(c.base, sliding, strconv.Itoa(readExpiryTTLSeconds), ""); err != nil {
		return err
	}
	if err := createExpiryStream(c.base, headOnly, strconv.Itoa(readExpiryTTLSeconds), ""); err != nil {
		return err
	}
	absoluteDeadline := time.Now().Add(6 * time.Second).UTC().Format(time.RFC3339)
	if err := createExpiryStream(c.base, absolute, "", absoluteDeadline); err != nil {
		return err
	}

	if err := requireStreamStatus(c.base, ordinary, http.MethodGet, http.StatusOK); err != nil {
		return err
	}
	if err := requireStreamStatus(c.base, ordinary, http.MethodGet, http.StatusOK); err != nil {
		return err
	}

	time.Sleep(2 * time.Second)
	if err := requireStreamStatus(c.base, sliding, http.MethodGet, http.StatusOK); err != nil {
		return fmt.Errorf("sliding read before original deadline: %w", err)
	}
	if err := requireStreamStatus(c.base, headOnly, http.MethodHead, http.StatusOK); err != nil {
		return fmt.Errorf("HEAD before sliding deadline: %w", err)
	}
	if err := requireStreamStatus(c.base, absolute, http.MethodGet, http.StatusOK); err != nil {
		return fmt.Errorf("read before absolute deadline: %w", err)
	}

	time.Sleep(2 * time.Second)
	if err := requireStreamStatus(c.base, sliding, http.MethodGet, http.StatusOK); err != nil {
		return fmt.Errorf("sliding read after original deadline: %w", err)
	}
	if err := requireStreamStatus(c.base, headOnly, http.MethodGet, http.StatusNotFound); err != nil {
		return fmt.Errorf("HEAD unexpectedly renewed sliding TTL: %w", err)
	}
	if err := requireStreamStatus(c.base, absolute, http.MethodGet, http.StatusOK); err != nil {
		return fmt.Errorf("absolute stream expired early: %w", err)
	}

	time.Sleep(3250 * time.Millisecond)
	if err := requireStreamStatus(c.base, sliding, http.MethodGet, http.StatusNotFound); err != nil {
		return fmt.Errorf("idle sliding stream did not expire: %w", err)
	}
	if err := requireStreamStatus(c.base, absolute, http.MethodGet, http.StatusNotFound); err != nil {
		return fmt.Errorf("reads moved absolute expiry: %w", err)
	}
	if err := requireStreamStatus(c.base, ordinary, http.MethodGet, http.StatusOK); err != nil {
		return fmt.Errorf("ordinary stream changed across repeated reads: %w", err)
	}

	fmt.Println("PASS: sliding reads renew, HEAD does not, absolute expiry stays fixed, ordinary reads persist")
	return nil
}

func createExpiryStream(base, stream, ttl, expiresAt string) error {
	req, err := http.NewRequest(
		http.MethodPut,
		fmt.Sprintf("%s/v1/stream/%s", base, stream),
		bytes.NewBufferString("x"),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	if ttl != "" {
		req.Header.Set("Stream-TTL", ttl)
	}
	if expiresAt != "" {
		req.Header.Set("Stream-Expires-At", expiresAt)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("create %s: %w", stream, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create %s: status %d", stream, resp.StatusCode)
	}
	return nil
}

func requireStreamStatus(base, stream, method string, want int) error {
	req, err := http.NewRequest(
		method,
		fmt.Sprintf("%s/v1/stream/%s?offset=-1", base, stream),
		nil,
	)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, stream, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: status %d, want %d", method, stream, resp.StatusCode, want)
	}
	return nil
}
