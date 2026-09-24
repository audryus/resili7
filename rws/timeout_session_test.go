package rws_test

import (
	"errors"
	"testing"
	"time"

	"codeberg.org/audryus/resili7/rws"
	"github.com/gorilla/websocket"
)

// H1: an already-expired session (outer RequestDeadline in the past) must be
// rejected before executing the handler.
func TestWsSessionPreCheckRejectsExpired(t *testing.T) {
	called := false
	mw := rws.NewTimeoutMiddleware(rws.TimeoutConfig{Session: time.Minute})
	h := mw(func(req rws.Request) (*rws.Response, error) {
		called = true
		return &rws.Response{MessageType: websocket.TextMessage}, nil
	})
	_, err := h(rws.Request{
		Now:             time.Now().UnixNano(),
		RequestDeadline: time.Now().Add(-time.Second).UnixNano(),
	})
	if !errors.Is(err, rws.ErrSessionTimeout) {
		t.Fatalf("expected ErrSessionTimeout, got %v", err)
	}
	if called {
		t.Fatal("handler executed despite expired session")
	}
}

// N3: Timeout middleware must set RequestDeadline from the session deadline.
func TestWsTimeoutPropagatesRequestDeadline(t *testing.T) {
	var gotDeadline int64
	mw := rws.NewTimeoutMiddleware(rws.TimeoutConfig{Session: time.Minute})
	h := mw(func(req rws.Request) (*rws.Response, error) {
		gotDeadline = req.RequestDeadline
		return &rws.Response{MessageType: websocket.TextMessage}, nil
	})
	now := time.Now().UnixNano()
	_, err := h(rws.Request{Now: now, MessageType: websocket.TextMessage})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := now + int64(time.Minute)
	if gotDeadline < want-int64(time.Second) || gotDeadline > want+int64(time.Second) {
		t.Fatalf("RequestDeadline not propagated: got %d want ~%d", gotDeadline, want)
	}
}

// Connection-close semantics: a session that expires during execution must
// return a zero response (nil) with ErrSessionTimeout.
func TestWsSessionExpiryReturnsZeroResponse(t *testing.T) {
	mw := rws.NewTimeoutMiddleware(rws.TimeoutConfig{Session: time.Nanosecond})
	h := mw(func(req rws.Request) (*rws.Response, error) {
		time.Sleep(10 * time.Millisecond)
		return &rws.Response{MessageType: websocket.TextMessage, Data: []byte("late")}, nil
	})
	resp, err := h(rws.Request{Now: time.Now().UnixNano()})
	if !errors.Is(err, rws.ErrSessionTimeout) {
		t.Fatalf("expected ErrSessionTimeout, got %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response on session timeout, got %+v", resp)
	}
}
