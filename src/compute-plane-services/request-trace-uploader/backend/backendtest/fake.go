// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package backendtest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
)

// ErrScripted is the cause of the error a failing SubmitStep returns when it
// names none. That error is a *backend.Fault that says what the outcome did:
// retryable for Refuse, which created nothing, and uncertain for LostResponse
// and AcceptWithError, which created a job.
var ErrScripted = errors.New("backendtest: scripted failure")

// Outcome is what the backend does with one Submit call.
type Outcome int

const (
	// Accept creates the job and returns its id. It is the zero value.
	Accept Outcome = iota
	// Refuse creates no job and returns the step's error.
	Refuse
	// LostResponse creates the job but returns no id and the step's error:
	// the upload committed and the caller never heard. Resubmitting the same
	// files then lands them twice.
	LostResponse
	// AcceptWithError creates the job and returns both its id and the step's
	// error. The caller must keep the id before looking at the error.
	AcceptWithError
)

// Landing says whether the files of a job the backend created reach the
// destination, independent of what its status script reports.
type Landing int

const (
	// LandsIfSucceeded lands the files once any status step reports success
	// with no error, whether or not anyone polls. It is the zero value.
	LandsIfSucceeded Landing = iota
	// Lands lands the files whatever the status script says, such as a job
	// that committed its data but reads as failed with upload_unconfirmed.
	Lands
	// NeverLands keeps the files from landing whatever the status script
	// says.
	NeverLands
)

// SubmitStep scripts one Submit call.
type SubmitStep struct {
	Outcome Outcome
	// Landing says whether the job this step creates lands its files.
	Landing Landing
	// Err is returned for every outcome except Accept. Nil means a Fault
	// wrapping ErrScripted, as ErrScripted describes.
	Err error
	// Delay holds the call before the backend acts, honoring the context. A
	// call canceled during the delay creates no job.
	Delay time.Duration
	// Statuses scripts Status calls for the job this step creates. Each call
	// plays the next step and the last step repeats, so a lone pending step
	// is a job stuck forever. Empty means one success step.
	Statuses []StatusStep
}

// StatusStep scripts one Status call. Status and Err are returned as given,
// so a step can be a read failure (no Status, an error) or a failure with its
// reason (StatusFailed and an error).
type StatusStep struct {
	Status backend.Status
	Err    error
	// Delay holds the call, honoring the context.
	Delay time.Duration
}

// Job is one job the backend created, including jobs whose id the caller
// never received.
type Job struct {
	ID          string
	Destination backend.Destination
	Files       []JobFile
	// Landed reports that the job's files reach the destination whether or
	// not anyone polls, as its step's Landing decides.
	Landed      bool
	StatusCalls int
	// ResubmitSafe reports that the backend was resubmit safe when the job
	// was submitted, so the destination kept one copy of a file it already
	// had.
	ResubmitSafe bool
}

// JobFile is one submitted file and the SHA-256 of its content when it was
// submitted. A file is identified by its path: the same path submitted again
// is the same file, while two paths with the same content are two files.
type JobFile struct {
	Path   string
	SHA256 string
}

// Fake is an in-memory asynchronous backend.Client whose outcomes a
// test scripts in advance with Script. It rejects requests that do not fit
// its capabilities or misreport a file's size, and records every submitted
// file, so a resubmit that lands a file twice in one destination shows in
// Duplicates.
// It is safe for concurrent use and registers nowhere; tests pass it to the
// code under test directly.
type Fake struct {
	mu          sync.Mutex
	caps        backend.Capabilities
	script      []SubmitStep
	jobs        []*fakeJob
	byID        map[string]*fakeJob
	submitCalls int
}

type fakeJob struct {
	job      Job
	statuses []StatusStep
}

// NewFake returns a backend that accepts every unscripted submit and
// reports success on the first poll. Its capabilities are those of an
// asynchronous exporting backend that takes one file per job, has one
// destination, and is not safe to resubmit to.
func NewFake() *Fake {
	return &Fake{
		caps: backend.Capabilities{
			AcceptedFormats: []backend.Format{backend.FormatGzipJSONL},
			Exports:         true,
		},
		byID: map[string]*fakeJob{},
	}
}

// SetCapabilities replaces what Capabilities reports and what Submit
// enforces.
func (f *Fake) SetCapabilities(caps backend.Capabilities) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.caps = cloneCapabilities(caps)
}

// Script queues steps for the next Submit calls, in order. Calls past the end
// of the script behave as an Accept step with no status script.
func (f *Fake) Script(steps ...SubmitStep) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, step := range steps {
		step.Statuses = slices.Clone(step.Statuses)
		f.script = append(f.script, step)
	}
}

// Submit plays the next scripted step for request. A request that does not
// fit the capabilities, a file that cannot be read, or a file whose declared
// size differs from its content fails the call without using a step. Steps
// go to calls in the order they enter Submit.
func (f *Fake) Submit(ctx context.Context, request backend.SubmitRequest) (string, error) {
	f.mu.Lock()
	f.submitCalls++
	caps := cloneCapabilities(f.caps)
	files, err := readFiles(caps, request)
	var step SubmitStep
	if err == nil && len(f.script) > 0 {
		step = f.script[0]
		f.script = f.script[1:]
	}
	f.mu.Unlock()
	if err != nil {
		return "", err
	}

	if err := wait(ctx, step.Delay); err != nil {
		return "", err
	}

	stepErr := step.Err
	if step.Outcome == Refuse {
		if stepErr == nil {
			stepErr = &backend.Fault{Class: backend.ClassRetryable, Scope: backend.ScopeUnknown, Reason: backend.ReasonUnknown, Err: ErrScripted}
		}
		return "", stepErr
	}
	if stepErr == nil {
		stepErr = &backend.Fault{Class: backend.ClassUncertain, Scope: backend.ScopeUnknown, Reason: backend.ReasonUnknown, Err: ErrScripted}
	}

	statuses := step.Statuses
	if len(statuses) == 0 {
		statuses = []StatusStep{{Status: backend.StatusSucceeded}}
	}

	f.mu.Lock()
	job := &fakeJob{
		job: Job{
			ID:           fmt.Sprintf("job-%06d", len(f.jobs)+1),
			Destination:  request.Destination,
			Files:        files,
			Landed:       lands(step.Landing, statuses),
			ResubmitSafe: caps.ResubmitSafe,
		},
		statuses: statuses,
	}
	f.jobs = append(f.jobs, job)
	f.byID[job.job.ID] = job
	f.mu.Unlock()

	switch step.Outcome {
	case LostResponse:
		return "", stepErr
	case AcceptWithError:
		return job.job.ID, stepErr
	default:
		return job.job.ID, nil
	}
}

// Status plays the job's next status step. An id the backend never issued
// reads as backend.StatusNotFound.
func (f *Fake) Status(ctx context.Context, id string) (backend.Status, error) {
	f.mu.Lock()
	job, ok := f.byID[id]
	if !ok {
		f.mu.Unlock()
		return backend.StatusNotFound, nil
	}
	step := job.statuses[min(job.job.StatusCalls, len(job.statuses)-1)]
	job.job.StatusCalls++
	f.mu.Unlock()

	if err := wait(ctx, step.Delay); err != nil {
		return "", err
	}
	return step.Status, step.Err
}

// Capabilities reports the configured capabilities.
func (f *Fake) Capabilities() backend.Capabilities {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneCapabilities(f.caps)
}

// Jobs returns every job created so far, in creation order.
func (f *Fake) Jobs() []Job {
	f.mu.Lock()
	defer f.mu.Unlock()
	jobs := make([]Job, 0, len(f.jobs))
	for _, job := range f.jobs {
		copied := job.job
		copied.Files = slices.Clone(copied.Files)
		jobs = append(jobs, copied)
	}
	return jobs
}

// Duplicates counts files in landed jobs that had already landed in the same
// destination in an earlier job. A file is identified by its path. A job
// submitted while the backend was resubmit safe adds no duplicate: the
// destination kept one copy.
func (f *Fake) Duplicates() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	type key struct {
		destination backend.Destination
		path        string
	}
	seen := map[key]bool{}
	duplicates := 0
	for _, job := range f.jobs {
		if !job.job.Landed {
			continue
		}
		for _, file := range job.job.Files {
			k := key{job.job.Destination, file.Path}
			if seen[k] && !job.job.ResubmitSafe {
				duplicates++
			}
		}
		for _, file := range job.job.Files {
			seen[key{job.job.Destination, file.Path}] = true
		}
	}
	return duplicates
}

// SubmitCalls counts every Submit call, including ones that failed.
func (f *Fake) SubmitCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.submitCalls
}

// readFiles checks request against caps and reads every file it names. The
// caller holds f.mu, so steps are dequeued in call order.
func readFiles(caps backend.Capabilities, request backend.SubmitRequest) ([]JobFile, error) {
	if err := caps.Check(request); err != nil {
		return nil, fmt.Errorf("backendtest: %w", err)
	}
	var files []JobFile
	for _, transport := range request.Transports() {
		content, err := os.ReadFile(transport.Path)
		if err != nil {
			return nil, fmt.Errorf("backendtest: read submitted file: %w", err)
		}
		if int64(len(content)) != transport.Bytes {
			return nil, fmt.Errorf("backendtest: submitted file is %d bytes, the request says %d", len(content), transport.Bytes)
		}
		sum := sha256.Sum256(content)
		files = append(files, JobFile{Path: transport.Path, SHA256: hex.EncodeToString(sum[:])})
	}
	return files, nil
}

// lands reports whether a job lands its files under landing and statuses.
func lands(landing Landing, statuses []StatusStep) bool {
	switch landing {
	case Lands:
		return true
	case NeverLands:
		return false
	}
	return slices.ContainsFunc(statuses, func(step StatusStep) bool {
		return step.Err == nil && step.Status == backend.StatusSucceeded
	})
}

func cloneCapabilities(caps backend.Capabilities) backend.Capabilities {
	caps.AcceptedFormats = slices.Clone(caps.AcceptedFormats)
	caps.Destinations = slices.Clone(caps.Destinations)
	return caps
}

func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
