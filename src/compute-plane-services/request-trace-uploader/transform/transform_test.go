// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package transform

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/compat"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
)

// memoryWriter is a fake format writer that keeps what it was given.
type memoryWriter struct {
	payloads []string
	records  []Record
	headers  [][]string
	closed   bool
	aborted  bool
	failOn   int
}

func (w *memoryWriter) Write(r Record) error {
	if w.failOn > 0 && len(w.payloads)+1 == w.failOn {
		return errors.New("disk full")
	}
	w.payloads = append(w.payloads, string(r.Payload))
	w.records = append(w.records, r)
	var headers []string
	for _, set := range r.Headers {
		headers = append(headers, string(compat.Encode(set)))
	}
	w.headers = append(w.headers, headers)
	return nil
}

func (w *memoryWriter) Close() error { w.closed = true; return nil }
func (w *memoryWriter) Abort() error { w.aborted = true; return nil }

func segmentFile(t *testing.T, lines ...string) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	for _, line := range lines {
		if _, err := gz.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return rawFile(t, buf.Bytes())
}

func rawFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "request-trace.000000.jsonl.gz")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func prepare(t *testing.T, cfg Config, path string) (Result, *memoryWriter, string) {
	t.Helper()
	preparer, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	writer := &memoryWriter{}
	var rejects bytes.Buffer
	result, err := preparer.Prepare(context.Background(), path, writer, &rejects)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	return result, writer, rejects.String()
}

const textLine = `{"schema":"dynamo.request.trace.v1", "event_type":"request_end","event_time_unix_ms":1,"request":{"request_id":"a"}}`

var pngURL = "data:image/png;base64," + base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{7, 99, 201}, 60)...))

func mediaLine(nca string) string {
	return `{"timestamp":5,"event":{"event_type":"request_payload","event_time_unix_ms":2,"payload":{"request_id":"m","http_request_headers":{"nvcf-ncaid":"` + nca +
		`"},"request":{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"` + pngURL + `"}}]}]}}}}`
}

func withSuppressed(ids ...string) Config {
	cfg := DefaultConfig()
	cfg.Suppression = suppress.FromIDs(ids)
	return cfg
}

func TestPrepareKeepsSourceBytesOfUnchangedRecords(t *testing.T) {
	result, writer, _ := prepare(t, DefaultConfig(), segmentFile(t, textLine))
	if len(writer.payloads) != 1 || writer.payloads[0] != textLine {
		t.Fatalf("payloads = %q, want the source line byte for byte", writer.payloads)
	}
	if result.Written != 1 || !writer.closed || writer.aborted {
		t.Fatalf("result = %+v closed = %v aborted = %v", result, writer.closed, writer.aborted)
	}
}

func TestPrepareRedactsMediaAndReserializes(t *testing.T) {
	result, writer, _ := prepare(t, DefaultConfig(), segmentFile(t, mediaLine("someone")))
	if result.RedactedParts != 1 || len(writer.payloads) != 1 {
		t.Fatalf("result = %+v, want one redacted part", result)
	}
	payload := writer.payloads[0]
	if strings.Contains(payload, "iVBOR") || !strings.Contains(payload, `"redacted":"multimodal"`) || !strings.Contains(payload, `"multimodal_redacted_parts":1`) {
		t.Fatalf("payload = %s, want the media replaced by a marker", payload)
	}
	if !writer.records[0].CarriesPayload {
		t.Error("CarriesPayload = false for a request_payload record")
	}
}

func TestPrepareSuppressesBeforeRedacting(t *testing.T) {
	result, writer, _ := prepare(t, withSuppressed("acme"), segmentFile(t, mediaLine("nca-acme-nca"), mediaLine("other")))
	if result.Suppressed != 1 || result.Evaluated != 2 || result.HeaderPresent != 2 || result.RedactedParts != 1 {
		t.Fatalf("result = %+v, want 1 suppressed, 2 evaluated with the header, 1 redacted", result)
	}
	stub := writer.payloads[0]
	if strings.Contains(stub, "multimodal") || strings.Contains(stub, "messages") || !strings.Contains(stub, `"payload_suppressed":true`) {
		t.Fatalf("stub = %s, want the body gone and no redaction marker", stub)
	}
	if !strings.HasPrefix(stub, `{"timestamp":5,"event":{`) {
		t.Errorf("stub = %.40s, want the wrapper timestamp kept", stub)
	}
}

func TestPrepareDropRecordRuleLeavesRecordOut(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Suppression = suppress.Config{Rules: []suppress.Rule{{Headers: suppress.DefaultHeaders, Values: []string{"acme"}, Action: suppress.DropRecord}}}
	result, writer, _ := prepare(t, cfg, segmentFile(t, mediaLine("acme"), textLine))
	if result.Dropped != 1 || result.Written != 1 || len(writer.payloads) != 1 || writer.payloads[0] != textLine {
		t.Fatalf("result = %+v payloads = %d, want the matching record dropped", result, len(writer.payloads))
	}
}

// A writer sees the header objects of the record it exports: redacted like
// the rest of it, and only the stub's headers for a suppressed record.
func TestPrepareHeadersAreThoseOfTheExportedRecord(t *testing.T) {
	media := `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-function-id":"fn","x-image":"` + pngURL + `"}}}`
	suppressed := `{"event_type":"request_payload","payload":{"request_id":"r1","http_request_headers":{"nvcf-ncaid":"acme","x-secret":"s3cr3t"}}}`
	_, writer, _ := prepare(t, withSuppressed("acme"), segmentFile(t, media, suppressed))
	if len(writer.headers) != 2 {
		t.Fatalf("headers = %v, want one set per record", writer.headers)
	}
	if got := strings.Join(writer.headers[0], ""); strings.Contains(got, "iVBOR") || !strings.Contains(got, "nvcf-function-id") {
		t.Errorf("headers = %s, want the header object with its media redacted", got)
	}
	if got := strings.Join(writer.headers[1], ""); got != `{"nvcf-ncaid":"acme"}` {
		t.Errorf("suppressed record headers = %s, want only the stub's header", got)
	}
}

// A line with a repeated key is never exported: the rules inspect the last
// value of a key, so an earlier one would leave unexamined.
func TestPrepareRejectsALineWithARepeatedKey(t *testing.T) {
	line := `{"event_type":"request_payload","payload":{"request_id":"r1","http_request_headers":{"nvcf-ncaid":"acme"},"request":"secret"},"payload":{"request_id":"r1"}}`
	result, writer, rejects := prepare(t, withSuppressed("acme"), segmentFile(t, line, textLine))
	if result.Unparseable != 1 || len(writer.payloads) != 1 || strings.Contains(strings.Join(writer.payloads, ""), "secret") {
		t.Fatalf("result = %+v payloads = %q, want the line rejected", result, writer.payloads)
	}
	if !strings.Contains(rejects, "secret") {
		t.Errorf("rejects = %q, want the raw line kept locally", rejects)
	}
}

// A suppressed record keeps request_id, endpoint and model; media there is
// redacted like anywhere else.
func TestPrepareRedactsTheStub(t *testing.T) {
	line := `{"event_type":"request_payload","payload":{"request_id":"r1","model":"` + pngURL + `","http_request_headers":{"nvcf-ncaid":"acme"}}}`
	result, writer, _ := prepare(t, withSuppressed("acme"), segmentFile(t, line))
	if result.Suppressed != 1 || strings.Contains(writer.payloads[0], "iVBOR") {
		t.Fatalf("result = %+v payload = %s, want the stub with its media redacted", result, writer.payloads[0])
	}
}

// A member whose checksum failed was changed on disk after its lines were
// read, so none of the segment is exported; it is quarantined whole.
func TestPrepareQuarantinesASegmentWithAChecksumFailure(t *testing.T) {
	var stored bytes.Buffer
	gz, err := gzip.NewWriterLevel(&stored, gzip.NoCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gz.Write([]byte(textLine + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	damaged := stored.Bytes()
	damaged[bytes.Index(damaged, []byte(`"a"`))+1] = 'b'
	good, err := os.ReadFile(segmentFile(t, textLine))
	if err != nil {
		t.Fatal(err)
	}
	result, writer, _ := prepare(t, DefaultConfig(), rawFile(t, append(damaged, good...)))
	if result.Quarantine != ReasonDamaged || result.ChecksumFailures != 1 {
		t.Fatalf("result = %+v, want quarantine %q", result, ReasonDamaged)
	}
	if !writer.aborted || writer.closed {
		t.Fatalf("writer closed = %v aborted = %v, want it aborted", writer.closed, writer.aborted)
	}
}

func TestPrepareChecksTheContextBeforeReading(t *testing.T) {
	long := strings.Repeat("x", 200)
	cfg := DefaultConfig()
	cfg.RawLineMax = 100
	preparer, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var rejects bytes.Buffer
	_, err = preparer.Prepare(ctx, segmentFile(t, long, long), &memoryWriter{}, &rejects)
	if !errors.Is(err, context.Canceled) || rejects.Len() != 0 {
		t.Fatalf("err = %v rejects = %d bytes, want context.Canceled before any reject write", err, rejects.Len())
	}
}

// cancelOnWrite is a reject writer that cancels the context on its first
// write and counts every write it gets.
type cancelOnWrite struct {
	cancel context.CancelFunc
	writes int
}

func (c *cancelOnWrite) Write(p []byte) (int, error) {
	c.writes++
	c.cancel()
	return len(p), nil
}

func TestPrepareStopsDrainingOversizedLinesOnCancel(t *testing.T) {
	long := strings.Repeat("x", 1<<20)
	cfg := DefaultConfig()
	cfg.RawLineMax = 100
	preparer, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rejects := &cancelOnWrite{cancel: cancel}
	writer := &memoryWriter{}
	_, err = preparer.Prepare(ctx, segmentFile(t, long, long, long, textLine), writer, rejects)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Prepare() error = %v, want context.Canceled", err)
	}
	if rejects.writes != 1 {
		t.Fatalf("reject writes = %d, want 1: the drain must stop at the first write after cancel", rejects.writes)
	}
	if !writer.aborted {
		t.Error("writer not aborted on cancel")
	}
}

func TestPrepareFingerprintIsFileHash(t *testing.T) {
	path := segmentFile(t, textLine, textLine)
	result, _, _ := prepare(t, DefaultConfig(), path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := sha256.Sum256(data); !bytes.Equal(result.Fingerprint, want[:]) {
		t.Fatalf("fingerprint = %x, want %x", result.Fingerprint, want)
	}
}

func TestPrepareRejectsMalformedLinesToArtifact(t *testing.T) {
	result, writer, rejects := prepare(t, DefaultConfig(), segmentFile(t, textLine, `{"broken`, "{\"bad\":\"\xff\"}", textLine))
	if result.Written != 2 || result.Unparseable != 2 || len(writer.payloads) != 2 {
		t.Fatalf("result = %+v, want 2 written and 2 unparseable", result)
	}
	if rejects != "{\"broken\n{\"bad\":\"\xff\"}\n" {
		t.Fatalf("rejects = %q, want both raw lines", rejects)
	}
	if result.Quarantine != "" {
		t.Errorf("quarantine = %q, want none at the default fraction", result.Quarantine)
	}
}

func TestPrepareHugeMediaLineRedactsUnderRowLimit(t *testing.T) {
	big := "data:image/png;base64," + base64.StdEncoding.EncodeToString(append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 3<<20)...))
	line := `{"event_type":"request_payload","payload":{"request":{"messages":[{"role":"user","content":"` + big + `"}]}}}`
	cfg := DefaultConfig()
	cfg.RowMax = 64 << 10
	result, writer, _ := prepare(t, cfg, segmentFile(t, line))
	if result.Written != 1 || result.RowTooLarge != 0 || len(writer.payloads[0]) > 1024 {
		t.Fatalf("result = %+v, want the line redacted to a small row", result)
	}
}

func TestPrepareRejectsRowOverRowLimit(t *testing.T) {
	long := `{"event_type":"request_end","note":"` + strings.Repeat("words ", 200) + `"}`
	cfg := DefaultConfig()
	cfg.RowMax = 512
	result, writer, rejects := prepare(t, cfg, segmentFile(t, long, textLine))
	if result.RowTooLarge != 1 || result.Written != 1 || len(writer.payloads) != 1 {
		t.Fatalf("result = %+v, want the long row rejected", result)
	}
	if rejects != long+"\n" {
		t.Errorf("rejects = %.40q, want the raw line", rejects)
	}
}

func TestPrepareRejectsLineOverRawLimit(t *testing.T) {
	long := `{"event_type":"request_end","note":"` + strings.Repeat("x", 300) + `"}`
	cfg := DefaultConfig()
	cfg.RawLineMax = 256
	cfg.RowMax = 256
	result, _, rejects := prepare(t, cfg, segmentFile(t, long, textLine))
	if result.LineTooLong != 1 || result.Written != 1 || rejects != long+"\n" {
		t.Fatalf("result = %+v rejects = %.30q, want the long line rejected whole", result, rejects)
	}
}

func TestPrepareQuarantinePastBadFraction(t *testing.T) {
	tests := []struct {
		name     string
		fraction float64
		good     int
		bad      int
		want     string
	}{
		{name: "every line bad", fraction: 1, bad: 3, want: ReasonBadRecords},
		{name: "one good line", fraction: 1, good: 1, bad: 99},
		{name: "half bad at half", fraction: 0.5, good: 2, bad: 2, want: ReasonBadRecords},
		{name: "under half", fraction: 0.5, good: 3, bad: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lines []string
			for i := 0; i < tt.good; i++ {
				lines = append(lines, textLine)
			}
			for i := 0; i < tt.bad; i++ {
				lines = append(lines, "not json")
			}
			cfg := DefaultConfig()
			cfg.MaxBadRecordFraction = tt.fraction
			result, writer, _ := prepare(t, cfg, segmentFile(t, lines...))
			if result.Quarantine != tt.want {
				t.Fatalf("quarantine = %q, want %q (result %+v)", result.Quarantine, tt.want, result)
			}
			if tt.want != "" && (!writer.aborted || writer.closed) {
				t.Errorf("aborted = %v closed = %v, want the transport discarded", writer.aborted, writer.closed)
			}
		})
	}
}

func TestPrepareQuarantinesOlderDynamoRecord(t *testing.T) {
	result, writer, _ := prepare(t, DefaultConfig(), segmentFile(t, textLine, `{"schema_version":1,"request_id":"r","model":"m"}`, textLine))
	if result.Quarantine != ReasonUnsupportedVersion || !writer.aborted || writer.closed {
		t.Fatalf("result = %+v aborted = %v, want the segment quarantined as unsupported", result, writer.aborted)
	}
}

func TestPrepareEmptySegmentHasNothingToUpload(t *testing.T) {
	path := rawFile(t, nil)
	result, writer, _ := prepare(t, DefaultConfig(), path)
	empty := sha256.Sum256(nil)
	if result.Written != 0 || result.Quarantine != "" || !bytes.Equal(result.Fingerprint, empty[:]) {
		t.Fatalf("result = %+v, want an empty, clean result", result)
	}
	if !writer.aborted || writer.closed {
		t.Errorf("aborted = %v closed = %v, want no transport for an empty segment", writer.aborted, writer.closed)
	}
}

func TestPrepareKeepsRecordsBeforeTruncatedTail(t *testing.T) {
	var buf bytes.Buffer
	for _, line := range []string{textLine, textLine} {
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
	}
	data := buf.Bytes()
	result, writer, _ := prepare(t, DefaultConfig(), rawFile(t, data[:len(data)-10]))
	if result.Written < 1 || result.LostBytes == 0 || result.Quarantine != "" || !writer.closed {
		t.Fatalf("result = %+v, want the first record kept and the tail counted", result)
	}
}

func TestPrepareQuarantinesUnreadableFile(t *testing.T) {
	result, writer, _ := prepare(t, DefaultConfig(), rawFile(t, []byte("not gzip at all\n")))
	if result.Quarantine != ReasonUnreadable || !writer.aborted {
		t.Fatalf("result = %+v, want a file with nothing readable quarantined", result)
	}
}

func TestPrepareAbortsWhenWriterFails(t *testing.T) {
	preparer, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	writer := &memoryWriter{failOn: 2}
	if _, err := preparer.Prepare(context.Background(), segmentFile(t, textLine, textLine), writer, io.Discard); err == nil {
		t.Fatal("Prepare() error = nil, want the writer error")
	}
	if !writer.aborted {
		t.Error("writer not aborted after a write error")
	}
}

func TestPrepareStopsOnCanceledContext(t *testing.T) {
	preparer, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer := &memoryWriter{}
	if _, err := preparer.Prepare(ctx, segmentFile(t, textLine), writer, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Prepare() error = %v, want context.Canceled", err)
	}
	if !writer.aborted {
		t.Error("writer not aborted on cancel")
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "defaults", mutate: func(*Config) {}},
		{name: "zero raw line max", mutate: func(c *Config) { c.RawLineMax = 0 }, wantErr: true},
		{name: "zero row max", mutate: func(c *Config) { c.RowMax = 0 }, wantErr: true},
		{name: "zero fraction", mutate: func(c *Config) { c.MaxBadRecordFraction = 0 }, wantErr: true},
		{name: "fraction above one", mutate: func(c *Config) { c.MaxBadRecordFraction = 1.5 }, wantErr: true},
		{name: "NaN fraction", mutate: func(c *Config) { c.MaxBadRecordFraction = math.NaN() }, wantErr: true},
		{name: "bad suppression rule", mutate: func(c *Config) {
			c.Suppression = suppress.Config{Rules: []suppress.Rule{{Action: "unknown"}}}
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if _, err := New(cfg); (err != nil) != tt.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPrepareLegacyQuarantineStillFingerprintsTheWholeFile(t *testing.T) {
	long := `{"event_type":"request_end","note":"` + strings.Repeat("x", 300) + `"}`
	tests := []struct {
		name         string
		lines        []string
		lineTooLong  int
		wantRejected string
	}{
		{name: "legacy first", lines: []string{`{"schema_version":1}`, `{}`, `not json`}},
		{name: "after a long line", lines: []string{long, textLine, `{"schema_version":1}`, `not json`}, lineTooLong: 1, wantRejected: long + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := segmentFile(t, tt.lines...)
			cfg := DefaultConfig()
			cfg.RawLineMax = 256
			result, writer, rejects := prepare(t, cfg, path)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if want := sha256.Sum256(data); !bytes.Equal(result.Fingerprint, want[:]) {
				t.Fatalf("fingerprint = %x, want the file hash %x", result.Fingerprint, want)
			}
			if result.Quarantine != ReasonUnsupportedVersion || !writer.aborted || writer.closed {
				t.Fatalf("result = %+v aborted = %v, want an unsupported-version quarantine", result, writer.aborted)
			}
			if result.LineTooLong != tt.lineTooLong || rejects != tt.wantRejected {
				t.Fatalf("line too long = %d rejects = %.40q, want %d and only lines before the legacy record", result.LineTooLong, rejects, tt.lineTooLong)
			}
		})
	}
}

// closeFailingWriter fails to finish its transport.
type closeFailingWriter struct{ memoryWriter }

func (w *closeFailingWriter) Close() error { return errors.New("disk full on finish") }

func TestPrepareAbortsWhenCloseFails(t *testing.T) {
	preparer, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	writer := &closeFailingWriter{}
	_, err = preparer.Prepare(context.Background(), segmentFile(t, textLine), writer, nil)
	if err == nil || !strings.Contains(err.Error(), "disk full on finish") {
		t.Fatalf("Prepare() error = %v, want the close error", err)
	}
	if !writer.aborted {
		t.Fatal("writer not aborted after Close failed")
	}
}

// A record with its own event_type is a v1.4.0 record, whatever its "event"
// field holds, as suppression reads it.
func TestPrepareKeepsARecordWithItsOwnEventTypeAndALegacyLookingEvent(t *testing.T) {
	line := `{"schema":"dynamo.request.trace.v1","event_type":"request_end","event_time_unix_ms":1,"event":{"schema_version":1}}`
	result, writer, _ := prepare(t, DefaultConfig(), segmentFile(t, textLine, line))
	if result.Quarantine != "" || !writer.closed || len(writer.payloads) != 2 {
		t.Fatalf("result = %+v payloads = %d, want both records written", result, len(writer.payloads))
	}
}

func TestPrepareQuarantinesAWrappedOlderDynamoRecord(t *testing.T) {
	result, writer, _ := prepare(t, DefaultConfig(), segmentFile(t, textLine, `{"timestamp":5,"event":{"schema_version":1,"request_id":"r"}}`))
	if result.Quarantine != ReasonUnsupportedVersion || !writer.aborted {
		t.Fatalf("result = %+v, want the segment quarantined as unsupported", result)
	}
}

// An "event" that is not an object does not wrap the record, so the record
// itself is read, as suppression reads it.
func TestPrepareReadsARecordWhoseEventIsNotAnObjectAsBare(t *testing.T) {
	for _, event := range []string{`null`, `"x"`, `[1]`} {
		line := `{"schema_version":1,"request_id":"r","event":` + event + `}`
		result, _, _ := prepare(t, DefaultConfig(), segmentFile(t, textLine, line))
		if result.Quarantine != ReasonUnsupportedVersion {
			t.Errorf("%s: result = %+v, want the segment quarantined as unsupported", line, result)
		}
	}
}
