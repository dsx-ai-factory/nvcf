// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Class says what a failed operation may have done at the destination. It
// decides whether the same work can be sent again.
type Class string

const (
	// ClassRetryable means nothing landed, so the same work can be sent
	// again after a backoff.
	ClassRetryable Class = "retryable"
	// ClassPermanent means sending the same work again fails the same way.
	ClassPermanent Class = "permanent"
	// ClassUncertain means the operation may or may not have landed, such as
	// a timeout after the request was sent. A read has no effect, so an
	// uncertain read is handled like a retryable one.
	ClassUncertain Class = "uncertain"
)

// Scope says how much work a fault affects.
type Scope string

const (
	// ScopeSegment means the fault belongs to the submitted data. Other
	// work is unaffected.
	ScopeSegment Scope = "segment"
	// ScopeBackend means the destination or the path to it is failing, so
	// every submit is affected. Nothing is quarantined for it.
	ScopeBackend Scope = "backend"
	// ScopeUnknown means it is not yet clear which of the two applies.
	ScopeUnknown Scope = "unknown"
)

// Reason is a short code for a fault. It comes from a small fixed set, so it
// is safe as a metric attribute and as the key that correlates faults across
// segments. The set covers job backends and object stores alike; a backend
// may define its own reason under the same rule (never an id, a path, a
// payload, or error text), and metrics count it as unknown.
type Reason string

const (
	// ReasonAuth means the destination refused the credentials. Backends use
	// it for every credential refusal, because it is always handled as
	// backend scope, whatever the Fault's Scope says: hold everything, never
	// split a batch, never quarantine.
	ReasonAuth Reason = "auth"
	// ReasonRateLimit means the destination asked the caller to slow down.
	ReasonRateLimit Reason = "rate_limit"
	// ReasonTimeout means the call ran out of time.
	ReasonTimeout Reason = "timeout"
	// ReasonInterrupted means the caller stopped the call, for example at
	// shutdown.
	ReasonInterrupted Reason = "interrupted"
	// ReasonJobCanceled means the destination canceled the job.
	ReasonJobCanceled Reason = "job_canceled"
	// ReasonNotFound means the configured bucket, table or namespace does
	// not exist. It is a configuration problem, so it is backend scope.
	ReasonNotFound Reason = "not_found"
	// ReasonProcessingFailed means the destination took the files but
	// failed to process them, so nothing was committed.
	ReasonProcessingFailed Reason = "processing_failed"
	// ReasonUploadUnconfirmed means the destination could not confirm that
	// the upload completed. The data may still land, so the job is sent
	// again only once, counted as a possible duplicate.
	ReasonUploadUnconfirmed Reason = "upload_unconfirmed"
	// ReasonBadResponse means the destination's answer could not be read.
	ReasonBadResponse Reason = "bad_response"
	// ReasonUnavailable means the destination or the network failed.
	ReasonUnavailable Reason = "unavailable"
	// ReasonRejected means the destination refused the request.
	ReasonRejected Reason = "rejected"
	// ReasonTooLarge means a file is over the backend's size limit.
	ReasonTooLarge Reason = "too_large"
	// ReasonUnknown means nothing recognized the error.
	ReasonUnknown Reason = "unknown"
)

// Fault is a classified backend error. A backend returns one, wrapped or not,
// when it knows what a failure means; Classify builds one for any other error.
// The zero Fault returned by Classify for a nil error means no fault. A Fault
// returned as an error with an empty Class is still a failure, and Classify
// reads it as uncertain.
type Fault struct {
	Class  Class
	Scope  Scope
	Reason Reason
	// RetryAfter is how long the destination asked the caller to wait, for
	// example from a Retry-After header. Zero means it did not say.
	RetryAfter time.Duration
	Err        error
}

// Error describes the fault and its cause. The cause can carry remote text,
// ids or paths, so logs and metrics use Class, Scope and Reason instead.
func (f *Fault) Error() string {
	if f == nil {
		return "backend fault (nil)"
	}
	text := fmt.Sprintf("backend fault (class=%s scope=%s reason=%s)", f.Class, f.Scope, f.Reason)
	if f.Err == nil {
		return text
	}
	return text + ": " + f.Err.Error()
}

// Unwrap returns the cause.
func (f *Fault) Unwrap() error {
	if f == nil {
		return nil
	}
	return f.Err
}

// asFault returns the first non-nil Fault in err's chain. A Fault with no
// Class still reports a failure, so it is read as uncertain, with unknown
// scope and reason where it gives none.
func asFault(err error) (Fault, bool) {
	var fault *Fault
	if !errors.As(err, &fault) || fault == nil {
		return Fault{}, false
	}
	found := *fault
	if found.Class == "" {
		found.Class = ClassUncertain
		if found.Scope == "" {
			found.Scope = ScopeUnknown
		}
		if found.Reason == "" {
			found.Reason = ReasonUnknown
		}
	}
	return found, true
}

// httpStatusError is implemented by HTTP client errors that expose the
// response status, such as the AWS SDK's response errors.
type httpStatusError interface {
	HTTPStatusCode() int
}

// ClassifyDefault maps an error that no backend classified. It reads only
// generic signals: a Fault already in the chain, context errors, an HTTP
// status, and network errors.
//
// A failure is retryable only when it shows that nothing was processed: a
// failed dial or name lookup, 408 or 429. A timeout, a broken connection
// after the request went out, any 5xx response (a proxy's 503 can follow a
// request that reached the destination), a status below 400 whose
// response could not be read, and any error it does not recognize are
// uncertain, because the destination may have acted before the failure. An
// unrecognized error has unknown scope, so correlation across segments
// decides its scope. A backend that knows better says so through its
// Classifier.
func ClassifyDefault(err error) Fault {
	if err == nil {
		return Fault{}
	}
	if fault, ok := asFault(err); ok {
		return fault
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) && sentNothing(err):
		// net reports a dial cut short by the caller's deadline as a dial
		// error that matches context.DeadlineExceeded. Nothing was sent.
		return Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable, Err: err}
	case errors.Is(err, context.DeadlineExceeded):
		return Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonTimeout, Err: err}
	case errors.Is(err, context.Canceled):
		return Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonInterrupted, Err: err}
	}
	var status httpStatusError
	if errors.As(err, &status) {
		return classifyHTTPStatus(status.HTTPStatusCode(), err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		switch {
		case sentNothing(err):
			return Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable, Err: err}
		case netErr.Timeout():
			return Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonTimeout, Err: err}
		default:
			return Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable, Err: err}
		}
	}
	return Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonUnknown, Err: err}
}

// sentNothing reports a network error from before any request byte left the
// host: a name lookup or a dial.
func sentNothing(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func classifyHTTPStatus(code int, err error) Fault {
	switch {
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonAuth, Err: err}
	case code == http.StatusTooManyRequests:
		return Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonRateLimit, Err: err}
	case code == http.StatusRequestTimeout:
		return Fault{Class: ClassRetryable, Scope: ScopeBackend, Reason: ReasonUnavailable, Err: err}
	case code >= 500:
		return Fault{Class: ClassUncertain, Scope: ScopeBackend, Reason: ReasonUnavailable, Err: err}
	case code >= 400:
		return Fault{Class: ClassPermanent, Scope: ScopeUnknown, Reason: ReasonRejected, Err: err}
	default:
		// A success or redirect status that still failed means the response
		// could not be read, such as a 200 with an undecodable body. The
		// request may have been processed.
		return Fault{Class: ClassUncertain, Scope: ScopeUnknown, Reason: ReasonBadResponse, Err: err}
	}
}

// ParseRetryAfter reads an HTTP Retry-After value, either delay seconds
// (digits only) or an HTTP date, as the wait from now. A date in the past
// means no wait. It reports false for a value it cannot read, so a backend can
// leave Fault.RetryAfter at zero. It does not cap the wait: a destination can
// ask for years, so whoever acts on Fault.RetryAfter caps it.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if strings.Trim(value, "0123456789") == "" {
		seconds, err := strconv.ParseInt(value, 10, 64)
		if err != nil || seconds > math.MaxInt64/int64(time.Second) {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	date, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(date.Sub(now), 0), true
}
