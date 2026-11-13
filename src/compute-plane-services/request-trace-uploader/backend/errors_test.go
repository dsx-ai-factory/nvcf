// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// statusError is the shape of an HTTP client error that exposes its response
// status, as the AWS SDK and other generated clients do.
type statusError struct{ code int }

func (e statusError) Error() string       { return fmt.Sprintf("http %d", e.code) }
func (e statusError) HTTPStatusCode() int { return e.code }

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func TestFaultWrapsCause(t *testing.T) {
	cause := errors.New("connection reset")
	err := fmt.Errorf("submit: %w", &Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable, Err: cause})

	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(%v, cause) = false, want true", err)
	}
	var fault *Fault
	if !errors.As(err, &fault) || fault.Reason != ReasonUnavailable {
		t.Fatalf("errors.As(%v) = %v, want the wrapped fault", err, fault)
	}
	for _, want := range []string{"retryable", "backend", "unavailable", "connection reset"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Error() = %q, want it to contain %q", err.Error(), want)
		}
	}
}

func TestClassifyDefault(t *testing.T) {
	wrapped := &Fault{Class: ClassPermanent, Scope: ScopeSegment, Reason: "custom", Err: errors.New("schema mismatch")}
	tests := []struct {
		name string
		err  error
		want Fault
	}{
		{name: "nil is no fault", err: nil, want: Fault{}},
		{name: "fault in the chain wins", err: fmt.Errorf("x: %w", wrapped), want: Fault{Class: ClassPermanent, Scope: ScopeSegment, Reason: "custom"}},
		{name: "deadline may have landed the call", err: fmt.Errorf("submit: %w", context.DeadlineExceeded), want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonTimeout}},
		{name: "cancel may have landed the call", err: context.Canceled, want: Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonInterrupted}},
		{name: "401 is backend auth", err: statusError{http.StatusUnauthorized}, want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonAuth}},
		{name: "403 is backend auth", err: fmt.Errorf("x: %w", statusError{http.StatusForbidden}), want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonAuth}},
		{name: "429 is a backend rate limit", err: statusError{http.StatusTooManyRequests}, want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonRateLimit}},
		{name: "408 is backend transient", err: statusError{http.StatusRequestTimeout}, want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "500 may have landed the call", err: statusError{http.StatusInternalServerError}, want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "502 may have landed the call", err: statusError{http.StatusBadGateway}, want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "504 may have landed the call", err: statusError{http.StatusGatewayTimeout}, want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "503 may have landed the call", err: statusError{http.StatusServiceUnavailable}, want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "400 is an unknown rejection", err: statusError{http.StatusBadRequest}, want: Fault{Class: ClassPermanent, Scope: ScopeUnknown, Reason: ReasonRejected}},
		{name: "404 is an unknown rejection", err: statusError{http.StatusNotFound}, want: Fault{Class: ClassPermanent, Scope: ScopeUnknown, Reason: ReasonRejected}},
		{name: "network timeout may have landed the call", err: &net.OpError{Op: "read", Err: timeoutError{}}, want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonTimeout}},
		{name: "refused connection is backend transient", err: &net.OpError{Op: "dial", Err: errors.New("connection refused")}, want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "dial timeout sent nothing", err: &net.OpError{Op: "dial", Err: timeoutError{}}, want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "DNS failure sent nothing", err: &net.DNSError{Err: "no such host", Name: "destination.invalid"}, want: Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "reset while reading the response may have landed the call", err: &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, want: Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable}},
		{name: "a 2xx whose body could not be read may have landed the call", err: statusError{http.StatusOK}, want: Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonBadResponse}},
		{name: "a truncated response may have landed the call", err: fmt.Errorf("decode create response: %w", io.ErrUnexpectedEOF), want: Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonUnknown}},
		{name: "anything else may have landed the call", err: errors.New("surprise"), want: Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonUnknown}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyDefault(tt.err)
			if got.Class != tt.want.Class || got.Scope != tt.want.Scope || got.Reason != tt.want.Reason {
				t.Fatalf("ClassifyDefault() = %s/%s/%s, want %s/%s/%s", got.Class, got.Scope, got.Reason, tt.want.Class, tt.want.Scope, tt.want.Reason)
			}
			if tt.err != nil && !errors.Is(got.Err, tt.err) && !errors.Is(tt.err, got.Err) {
				t.Fatalf("ClassifyDefault().Err = %v, want the classified error", got.Err)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		value  string
		want   time.Duration
		wantOK bool
	}{
		{name: "seconds", value: "120", want: 120 * time.Second, wantOK: true},
		{name: "seconds with spaces", value: " 5 ", want: 5 * time.Second, wantOK: true},
		{name: "zero", value: "0", want: 0, wantOK: true},
		{name: "future date", value: now.Add(90 * time.Second).Format(http.TimeFormat), want: 90 * time.Second, wantOK: true},
		{name: "past date", value: now.Add(-time.Minute).Format(http.TimeFormat), want: 0, wantOK: true},
		{name: "empty", value: "", wantOK: false},
		{name: "negative", value: "-1", wantOK: false},
		{name: "garbage", value: "soon", wantOK: false},
		{name: "overflows a duration", value: "99999999999", wantOK: false},
		{name: "a sign is not delay seconds", value: "+5", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseRetryAfter(tt.value, now)
			if ok != tt.wantOK || got != tt.want {
				t.Fatalf("ParseRetryAfter(%q) = %v, %v, want %v, %v", tt.value, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

// A dial cut short by the caller's deadline is the error net returns itself,
// which matches context.DeadlineExceeded. Nothing was sent, so it stays
// retryable.
func TestClassifyDefaultRealDialDeadlineIsRetryable(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := (&net.Dialer{}).DialContext(ctx, "tcp", "192.0.2.1:9")
	if err == nil {
		t.Fatal("DialContext() with an expired deadline succeeded")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "dial" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DialContext() error = %#v, want a dial *net.OpError matching context.DeadlineExceeded", err)
	}
	got := ClassifyDefault(fmt.Errorf("submit: %w", err))
	if got.Class != ClassRetryable || got.Scope != ScopeBackend || got.Reason != ReasonUnavailable {
		t.Fatalf("ClassifyDefault(dial deadline) = %s/%s/%s, want retryable/backend/unavailable", got.Class, got.Scope, got.Reason)
	}
}

func TestClassifyDefaultDeadlineAfterSendStaysAmbiguous(t *testing.T) {
	got := ClassifyDefault(fmt.Errorf("poll: %w", context.DeadlineExceeded))
	if got.Class != ClassUncertain || got.Reason != ReasonTimeout {
		t.Fatalf("ClassifyDefault(deadline) = %s/%s, want ambiguous/timeout", got.Class, got.Reason)
	}
}

// A backend that returns a typed nil *Fault hands back a non-nil error. It is
// not a fault, so the default rules classify it, without a panic.
func TestClassifyDefaultNilFault(t *testing.T) {
	var fault *Fault
	var err error = fault
	got := ClassifyDefault(err)
	if got.Class != ClassUncertain || got.Scope != ScopeUnknown || got.Reason != ReasonUnknown {
		t.Fatalf("ClassifyDefault(nil *Fault) = %s/%s/%s, want uncertain/unknown/unknown", got.Class, got.Scope, got.Reason)
	}
	if text := fault.Error(); text == "" {
		t.Fatal("(*Fault)(nil).Error() is empty")
	}
	if fault.Unwrap() != nil {
		t.Fatal("(*Fault)(nil).Unwrap() is not nil")
	}
}

// A Fault with no Class still says the call failed, so it must never read
// as no fault. It is uncertain, keeping any scope and reason it gives.
func TestClassifyFaultWithoutClassIsUncertain(t *testing.T) {
	tests := []struct {
		name  string
		fault *Fault
		want  Fault
	}{
		{name: "empty", fault: &Fault{}, want: Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonUnknown}},
		{name: "scope and reason kept", fault: &Fault{Scope: ScopeSegment, Reason: ReasonRejected}, want: Fault{Class: ClassUncertain, Scope: ScopeSegment, Reason: ReasonRejected}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, got := range []Fault{ClassifyDefault(fmt.Errorf("submit: %w", tt.fault)), Classify(plainClient{}, tt.fault)} {
				if got.Class != tt.want.Class || got.Scope != tt.want.Scope || got.Reason != tt.want.Reason {
					t.Fatalf("classified = %s/%s/%s, want %s/%s/%s", got.Class, got.Scope, got.Reason, tt.want.Class, tt.want.Scope, tt.want.Reason)
				}
			}
		})
	}
}
