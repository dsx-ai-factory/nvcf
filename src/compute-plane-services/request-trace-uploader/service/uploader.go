// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/internal/health"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
)

// DefaultHeaderCheckRecords is how many payload records the uploader reads,
// with suppression on, before it warns that none carried a rule's header.
const DefaultHeaderCheckRecords = 100

// Options tunes an Uploader beyond its configuration.
type Options struct {
	// Metrics receives the pipeline's observations. Nil records nothing.
	Metrics telemetry.Metrics
	// HeaderCheckRecords overrides DefaultHeaderCheckRecords.
	HeaderCheckRecords int
}

// Uploader runs the export pipeline. For each closed segment it applies
// suppression and then redaction to every record, writes the records as a
// transport file, submits that file, and deletes the source once the backend
// confirms a real export.
type Uploader struct {
	cfg      config.Config
	client   backend.Client
	preparer *transform.Preparer
	metrics  telemetry.Metrics
	rules    suppressionRules
	watch    *headerWatch
	health   *health.Handler
}

// NewUploader builds the pipeline around an already-built backend. The
// pipeline submits each segment to one destination and keeps no record of a
// submit between passes, so a segment it cannot confirm is submitted again.
// It therefore refuses a backend that declares more than one destination or
// is not resubmit safe.
func NewUploader(cfg config.Config, client backend.Client, opts Options) (*Uploader, error) {
	if client == nil {
		return nil, errors.New("request trace uploader needs a backend")
	}
	caps := client.Capabilities()
	if n := len(caps.AllDestinations()); n != 1 {
		return nil, fmt.Errorf("request trace uploader supports one backend destination, the backend declares %d", n)
	}
	if !caps.ResubmitSafe {
		return nil, errors.New("request trace uploader needs a resubmit-safe backend: a segment it cannot confirm is submitted again")
	}
	rules, err := newSuppressionRules(cfg.Suppression)
	if err != nil {
		return nil, err
	}
	prepareConfig := transform.DefaultConfig()
	prepareConfig.Suppression = cfg.Suppression
	prepareConfig.Redact = !cfg.SkipRedaction
	preparer, err := transform.New(prepareConfig)
	if err != nil {
		return nil, err
	}
	metrics := opts.Metrics
	if metrics == nil {
		metrics = discardMetrics{}
	}
	metrics = telemetry.Safe(metrics)
	telemetry.Preset(metrics)
	rules.preset(metrics)
	threshold := opts.HeaderCheckRecords
	if threshold <= 0 {
		threshold = DefaultHeaderCheckRecords
	}
	return &Uploader{
		cfg:      cfg,
		client:   client,
		preparer: preparer,
		metrics:  metrics,
		rules:    rules,
		watch:    newHeaderWatch(preparer.SuppressionActive(), threshold, rules.names()),
		health:   health.New(),
	}, nil
}

// Handler returns the health endpoints.
func (u *Uploader) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", u.health.Live)
	mux.HandleFunc("GET /readyz", u.health.Ready)
	return mux
}

// Run serves health at once, reports ready after the local startup steps,
// and then runs a pass every scan interval until ctx ends. A failed pass is
// logged and retried on the next tick: a backend or spool fault never stops
// the process.
func (u *Uploader) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", u.cfg.HealthAddr)
	if err != nil {
		return fmt.Errorf("listen for uploader health checks: %w", err)
	}
	server := newHTTPServer(u.cfg.HealthAddr, u.Handler())
	errs := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Warn("shut down uploader health server", "error", err)
		}
	}()

	if err := u.health.Start(ctx,
		health.Step{Name: "create local directories", Run: u.createDirectories},
		health.Step{Name: "check the secrets file", Run: u.checkSecretsFile},
	); err != nil {
		return err
	}

	ticker := time.NewTicker(u.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		if err := u.Pass(ctx); err != nil && ctx.Err() == nil {
			slog.Error("request trace pass failed", "error", safeError(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errs:
			return fmt.Errorf("serve uploader health checks: %w", err)
		case <-ticker.C:
		}
	}
}

func (u *Uploader) createDirectories(context.Context) error {
	for _, directory := range []string{u.cfg.StateDir, u.cfg.QuarantineDir} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return fmt.Errorf("create uploader directory: %w", err)
		}
	}
	return nil
}

func (u *Uploader) checkSecretsFile(context.Context) error {
	secret, err := os.Open(u.cfg.SecretsFile)
	if err != nil {
		return fmt.Errorf("open uploader secret file: %w", err)
	}
	return secret.Close()
}

// Pass exports every closed segment once. A segment that fails is logged
// and left for the next pass.
func (u *Uploader) Pass(ctx context.Context) error {
	scan, err := segment.ScanDir(u.cfg.SourceDir, u.cfg.Prefixes())
	if err != nil {
		return fmt.Errorf("discover request trace segments: %w", err)
	}
	for _, item := range scan.Closed {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := u.export(ctx, item); err != nil {
			slog.Error("export request trace segment", "segment", item.Index, "bytes", item.Size, "error", safeError(err))
		}
	}
	return nil
}

func (u *Uploader) export(ctx context.Context, item segment.Segment) error {
	transportDir := filepath.Join(u.cfg.StateDir, "transport")
	if err := os.MkdirAll(transportDir, 0o750); err != nil {
		return fmt.Errorf("create transport directory: %w", err)
	}
	name := filepath.Base(item.Path)
	path := filepath.Join(transportDir, name)
	writer, err := transform.CreateJSONLFile(path)
	if err != nil {
		return err
	}
	rejects := transform.NewRejectFile(filepath.Join(u.cfg.QuarantineDir, "rejects"))
	result, err := u.preparer.Prepare(ctx, item.Path, writer, rejects)
	if err != nil {
		return errors.Join(fmt.Errorf("prepare segment: %w", err), rejects.Discard())
	}
	if _, err := rejects.Commit(strings.TrimSuffix(name, ".jsonl.gz")); err != nil {
		// The rejected lines exist only in the source until they are kept,
		// so the segment stays for the next pass.
		return fmt.Errorf("keep rejected lines: %w", err)
	}

	if result.Quarantine != "" {
		slog.Warn("request trace segment not exported", "segment", item.Index, "reason", result.Quarantine)
		if err := u.quarantine(item); err != nil {
			return err
		}
		u.recordPrepared(result)
		return nil
	}
	caps := u.client.Capabilities()
	if result.Written == 0 {
		u.recordPrepared(result)
		if caps.Exports {
			return removeSource(item.Path)
		}
		return nil
	}
	defer func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			slog.Warn("remove request trace transport", "segment", item.Index, "error", safeError(err))
		}
	}()
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat transport: %w", err)
	}
	id, err := u.client.Submit(ctx, backend.SubmitRequest{
		Segment:     item,
		Path:        path,
		Destination: caps.AllDestinations()[0],
		Files:       []backend.File{{Path: path, Bytes: info.Size(), Rows: int64(result.Written)}},
	})
	if err != nil {
		return backendError("submit segment to backend", u.client, err)
	}
	status, err := u.client.Status(ctx, id)
	if err != nil {
		return backendError("read backend status", u.client, err)
	}
	slog.Info("submitted request trace segment", "segment", item.Index, "records", result.Written, "status", status)
	if status != backend.StatusSucceeded {
		return nil
	}
	// The counts are recorded once the segment's outcome is final, so a
	// segment tried on several passes is counted once. A backend that does
	// not export reads the segment again on every pass and counts it each
	// time.
	u.recordPrepared(result)
	if !caps.Exports {
		return nil
	}
	return removeSource(item.Path)
}

// quarantine moves a segment that cannot be exported out of the spool, so it
// is not read again on every pass. A file already there is not replaced.
func (u *Uploader) quarantine(item segment.Segment) error {
	target := filepath.Join(u.cfg.QuarantineDir, filepath.Base(item.Path))
	if _, err := os.Lstat(target); err == nil {
		return fmt.Errorf("quarantine segment: a quarantined file of that name exists")
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("quarantine segment: %w", err)
	}
	if err := os.Rename(item.Path, target); err != nil {
		return fmt.Errorf("quarantine segment: %w", err)
	}
	return nil
}

// backendError describes a backend failure by its fault class, scope and
// reason. The cause can carry paths, URLs or job ids, so it is left out.
func backendError(op string, client backend.Client, err error) error {
	fault := backend.Classify(client, err)
	return fmt.Errorf("%s: backend fault (class=%s scope=%s reason=%s)", op, fault.Class, fault.Scope, fault.Reason)
}

// safeError describes err for a log line without the paths that file
// system errors carry.
func safeError(err error) string {
	if err == nil {
		return ""
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var parts []string
		for _, e := range joined.Unwrap() {
			if e != nil {
				parts = append(parts, safeError(e))
			}
		}
		return strings.Join(parts, "; ")
	}
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Op + ": " + pathErr.Err.Error()
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return linkErr.Op + ": " + linkErr.Err.Error()
	}
	return err.Error()
}

func removeSource(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete exported segment: %w", err)
	}
	return nil
}

type discardMetrics struct{}

func (discardMetrics) Add(string, float64, telemetry.Labels)     {}
func (discardMetrics) Set(string, float64, telemetry.Labels)     {}
func (discardMetrics) Observe(string, float64, telemetry.Labels) {}
