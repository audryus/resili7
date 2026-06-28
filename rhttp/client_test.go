package rhttp_test

import (
	"net/http"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rhttp"
)

func TestNewClientUsesConfiguredFanoutLimit(t *testing.T) {
	client, err := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler: func(r rhttp.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK}, nil
		},
		RetryHedgeFanoutLimit: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected client to be created")
	}
}

func TestRetry429PolicySelection(t *testing.T) {
	predicate := rhttp.Retry429PolicyFor(rhttp.Retry429Always)
	resp := &http.Response{StatusCode: http.StatusTooManyRequests}
	if !predicate(resp, nil) {
		t.Fatal("expected 429 policy to allow retry when configured")
	}
}

func TestRetry429PolicyDefault(t *testing.T) {
	predicate := rhttp.Retry429PolicyFor(rhttp.Retry429Default)
	resp := &http.Response{StatusCode: http.StatusTooManyRequests}
	if predicate(resp, nil) {
		t.Fatal("expected default 429 policy to avoid retry")
	}
}

func TestNewClientAcceptsPipelineOptions(t *testing.T) {
	_, err := rhttp.NewClient(rhttp.Pipeline{
		HttpHandler:           func(r rhttp.Request) (*http.Response, error) { return &http.Response{StatusCode: http.StatusOK}, nil },
		RetryHedgeFanoutLimit: 2,
		Retry429Policy:        rhttp.Retry429Always,
		Timeout:               rhttp.NewTimeoutMiddleware(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
