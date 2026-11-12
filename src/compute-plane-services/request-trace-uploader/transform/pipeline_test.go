// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package transform_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/segment"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
)

// rowWriter is a format writer that keeps each closed transport's rows in
// memory, one payload per row.
type rowWriter struct {
	rows   []string
	closed func([]string)
}

func (w *rowWriter) Write(r transform.Record) error {
	w.rows = append(w.rows, string(r.Payload))
	return nil
}

func (w *rowWriter) Close() error { w.closed(w.rows); return nil }
func (w *rowWriter) Abort() error { return nil }

// prepareSink is the hand-off from intake to prepare: each closed segment
// is prepared into a transport, kept here by segment index.
type prepareSink struct {
	preparer   *transform.Preparer
	results    map[int]transform.Result
	transports map[int][]string
}

func (s *prepareSink) Accept(ctx context.Context, item segment.Segment) error {
	writer := &rowWriter{closed: func(rows []string) { s.transports[item.Index] = rows }}
	result, err := s.preparer.Prepare(ctx, item.Path, writer, nil)
	if err != nil {
		return err
	}
	s.results[item.Index] = result
	return nil
}

type openGate struct{}

func (openGate) Admit(segment.Segment) bool { return true }

func writeSegment(t *testing.T, path string, lines ...string) {
	t.Helper()
	var buf bytes.Buffer
	for _, line := range lines {
		// One gzip member per line, as one writer flush each.
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSpoolToTransport(t *testing.T) {
	spool := t.TempDir()
	png := "data:image/png;base64," + base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{1, 2, 3}, 100)...))
	payload := func(nca string) string {
		return `{"timestamp":9,"event":{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,` +
			`"payload":{"request_id":"r","http_request_headers":{"nvcf-ncaid":"` + nca + `"},"request":{"messages":[{"role":"user","content":[` +
			`{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"` + png + `"}}]}]},"payload_complete":true}}}`
	}
	end := `{"timestamp":10,"event":{"schema":"dynamo.request.trace.v1","event_type":"request_end","event_time_unix_ms":1777312801001,"request":{"request_id":"r"}}}`
	writeSegment(t, filepath.Join(spool, "request-trace.999999.jsonl.gz"), payload("customer"), end)
	writeSegment(t, filepath.Join(spool, "request-trace.1000000.jsonl.gz"), payload("nca-blocked-nca"), "not json", end)
	writeSegment(t, filepath.Join(spool, "request-trace.1000001.jsonl.gz"), end)

	cfg := transform.DefaultConfig()
	cfg.Suppression = suppress.FromIDs([]string{"blocked"}, "nvcf-ncaid", "nvcf-nca-id")
	preparer, err := transform.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sink := &prepareSink{preparer: preparer, results: map[int]transform.Result{}, transports: map[int][]string{}}
	intake, err := segment.NewIntake(spool, segment.DefaultConfig("request-trace"), sink, openGate{})
	if err != nil {
		t.Fatal(err)
	}

	pass, err := intake.Pass(context.Background())
	if err != nil {
		t.Fatalf("Pass() error = %v", err)
	}
	if pass.Handed != 2 || len(pass.Scan.Active) != 1 || pass.Scan.Active[0].Index != 1000001 {
		t.Fatalf("pass = %+v, want two closed segments handed and the newest left active", pass)
	}
	if _, ok := sink.transports[1000001]; ok {
		t.Fatal("active segment was prepared")
	}

	first := sink.results[999999]
	if first.Written != 2 || first.RedactedParts != 1 || first.Suppressed != 0 || first.Quarantine != "" {
		t.Fatalf("first segment = %+v", first)
	}
	rows := sink.transports[999999]
	if len(rows) != 2 || strings.Contains(rows[0], "iVBOR") || !strings.Contains(rows[0], `"redacted":"multimodal"`) || rows[1] != end {
		t.Fatalf("first transport = %q", rows)
	}

	second := sink.results[1000000]
	if second.Written != 2 || second.Suppressed != 1 || second.Unparseable != 1 || second.RedactedParts != 0 {
		t.Fatalf("second segment = %+v", second)
	}
	rows = sink.transports[1000000]
	if !strings.Contains(rows[0], `"payload_suppressed":true`) || strings.Contains(rows[0], "messages") {
		t.Fatalf("second transport stub = %s", rows[0])
	}
	if bytes.Equal(first.Fingerprint, second.Fingerprint) || len(first.Fingerprint) != 32 {
		t.Fatalf("fingerprints %x and %x, want two distinct SHA-256 values", first.Fingerprint, second.Fingerprint)
	}
	for _, name := range []string{"request-trace.999999.jsonl.gz", "request-trace.1000000.jsonl.gz", "request-trace.1000001.jsonl.gz"} {
		if _, err := os.Stat(filepath.Join(spool, name)); err != nil {
			t.Errorf("intake or prepare changed the spool: %v", err)
		}
	}
}
