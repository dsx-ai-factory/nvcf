// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package service_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/backend/backendtest"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/config"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/service"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/telemetry"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/uploadertest"
)

// The suppressed line and its stub are the first case of
// transform/suppress/testdata/python.json, which the earlier shell
// exporter's Python step produced byte for byte.
const (
	suppressedLine = `{"timestamp":101,"event":{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,"event_source":"frontend","payload":{"request_id":"r1","endpoint":"chat","model":"m","request":{"messages":[{"role":"user","content":"secret"}]},"response":{"choices":[]},"http_request_headers":{"nvcf-ncaid":"nca-acme-nca"},"payload_complete":true}}}`
	shellStub      = `{"timestamp":101,"event":{"schema":"dynamo.request.trace.v1","event_type":"request_payload","event_time_unix_ms":1777312801000,"event_source":"frontend","payload":{"request_id":"r1","endpoint":"chat","model":"m","payload_complete":false,"payload_drop_reason":"nca_id","payload_suppressed":true,"suppression_reason":"nca_id","nca_id":"acme","http_request_headers":{"nvcf-ncaid":"acme"}}}}`
)

func TestChartSuppressionEnvExportsTheShellStub(t *testing.T) {
	keptPayload := uploadertest.RequestPayload("r2", 1777312802000, map[string]string{"nvcf-ncaid": "nca-beta-nca"})
	keptEnd := uploadertest.RequestEnd("r1", 1777312803000)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(suppressedLine, keptPayload, keptEnd).Bytes())
	active := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("r3", 1)).Bytes())

	// The chart renders suppressedNcaIds with `| quote`, so the container
	// sees the comma-separated list as it was written in the values.
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs: "nca-acme-nca,other-org",
	}))
	exporter := newCapture(t, resubmitSafeFake())
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := service.NewUploader(cfg, exporter, service.Options{Metrics: metrics})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}

	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}

	exported := exporter.exported()
	if want := []string{shellStub, keptPayload, keptEnd}; !slices.Equal(exported, want) {
		t.Fatalf("exported =\n%s\nwant\n%s", strings.Join(exported, "\n"), strings.Join(want, "\n"))
	}
	if strings.Contains(strings.Join(exported, "\n"), "secret") {
		t.Fatal("the suppressed request body was exported")
	}
	for name, labels := range map[string]telemetry.Labels{
		telemetry.RecordsSuppressed:        {"rule": config.NCAIDRuleName, "action": "strip_payload"},
		telemetry.SuppressionHeaderPresent: {"rule": config.NCAIDRuleName},
	} {
		want := map[string]float64{telemetry.RecordsSuppressed: 1, telemetry.SuppressionHeaderPresent: 2}[name]
		if got, _ := metrics.Value(name, labels); got != want {
			t.Errorf("%s%v = %v, want %v", name, labels, got, want)
		}
	}
	if got, _ := metrics.Value(telemetry.RecordsEvaluated, telemetry.Labels{"record_type": "request_payload"}); got != 2 {
		t.Errorf("records_evaluated = %v, want 2", got)
	}
	if _, err := os.Stat(closed); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("closed segment after a confirmed export: %v, want removed", err)
	}
	if _, err := os.Stat(active); err != nil {
		t.Errorf("active segment: %v, want it left for the writer", err)
	}
}

// A stripPayload rule from the rules setting matches on a header that is not
// the NCA id, so its stub must not present the matched value as one.
func TestCustomStripRuleStubCarriesNoIdentifier(t *testing.T) {
	line := uploadertest.RequestPayload("r1", 1777312801000, map[string]string{"x-user-id": "alice@example.com", "nvcf-ncaid": "nca-beta-nca"})
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Roll(uploadertest.NewSegment().Member(line).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("r2", 1)).Bytes())

	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressionRules: `[{"name":"users","headers":["x-user-id"],"values":["alice@example.com"],"action":"stripPayload"}]`,
	}))
	exporter := newCapture(t, resubmitSafeFake())
	uploader, err := service.NewUploader(cfg, exporter, service.Options{})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatalf("Pass() error = %v", err)
	}

	exported := strings.Join(exporter.exported(), "\n")
	for _, leak := range []string{"alice@example.com", `"nca_id"`, "nvcf-ncaid", "beta"} {
		if strings.Contains(exported, leak) {
			t.Errorf("exported stub contains %s:\n%s", leak, exported)
		}
	}
	if !strings.Contains(exported, `"suppression_reason":"users"`) {
		t.Errorf("exported = %s, want the rule name as the suppression reason", exported)
	}
}

func TestSuppressionCountersArePresetAndCarryOnlyRuleNames(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:   "acme",
		config.EnvSuppressionRules: `[{"name":"tenants","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`,
	}))
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.NewUploader(cfg, resubmitSafeFake(), service.Options{Metrics: metrics}); err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}

	for _, want := range []struct {
		name   string
		labels telemetry.Labels
	}{
		{telemetry.RecordsSuppressed, telemetry.Labels{"rule": "nca_id", "action": "strip_payload"}},
		{telemetry.RecordsSuppressed, telemetry.Labels{"rule": "tenants", "action": "drop_record"}},
		{telemetry.SuppressionHeaderPresent, telemetry.Labels{"rule": "nca_id"}},
		{telemetry.SuppressionHeaderPresent, telemetry.Labels{"rule": "tenants"}},
	} {
		if value, ok := metrics.Value(want.name, want.labels); !ok || value != 0 {
			t.Errorf("%s%v = %v, %v; want a zero series before any record", want.name, want.labels, value, ok)
		}
	}
	for _, name := range []string{telemetry.RecordsSuppressed, telemetry.SuppressionHeaderPresent} {
		for _, labels := range metrics.Series(name) {
			for key, value := range labels {
				if strings.Contains(value, "ncaid") || strings.Contains(value, "tenant-") || value == "acme" || value == "t1" || value == "x-tenant" {
					t.Errorf("%s label %s=%q exposes a header name or value", name, key, value)
				}
			}
		}
	}
}

func TestWarnsOnceWhenNoSuppressionHeaderIsSeen(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvSuppressNCAIDs: "acme"}))
	exporter := newCapture(t, resubmitSafeFake())
	uploader, err := service.NewUploader(cfg, exporter, service.Options{HeaderCheckRecords: 3})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	headerless := func(id string) string { return uploadertest.RequestPayload(id, 1, nil) }
	const warning = "no payload record carried a suppression header"

	spool.Roll(uploadertest.NewSegment().Member(headerless("a"), headerless("b")).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), warning) {
		t.Fatalf("warned after 2 payload records, want a warning only after 3:\n%s", logs)
	}

	for range 2 {
		spool.Roll(uploadertest.NewSegment().Member(headerless("c"), headerless("d")).Bytes())
		if err := uploader.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Count(logs.String(), warning); got != 1 {
		t.Fatalf("warnings = %d, want exactly 1:\n%s", got, logs)
	}
	// The condition only warns: every record of the closed segments (a, b,
	// x, c, d) is still exported.
	if got := len(exporter.exported()); got != 5 {
		t.Fatalf("exported %d records, want all 5", got)
	}
}

func TestNoHeaderWarningOnceAHeaderIsSeen(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvSuppressNCAIDs: "acme"}))
	uploader, err := service.NewUploader(cfg, resubmitSafeFake(), service.Options{HeaderCheckRecords: 2})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	spool.Roll(uploadertest.NewSegment().Member(
		uploadertest.RequestPayload("a", 1, map[string]string{"nvcf-ncaid": "other"}),
		uploadertest.RequestPayload("b", 1, nil),
		uploadertest.RequestPayload("c", 1, nil),
	).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "suppression header") {
		t.Fatalf("warned although a header was seen:\n%s", logs)
	}
}

// Each rule needs its own header: one rule's header being present says
// nothing about another rule that can never fire.
func TestWarnsForEachRuleWhoseHeaderIsNeverSeen(t *testing.T) {
	logs := captureLogs(t)
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{
		config.EnvSuppressNCAIDs:   "acme",
		config.EnvSuppressionRules: `[{"name":"tenants","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`,
	}))
	uploader, err := service.NewUploader(cfg, resubmitSafeFake(), service.Options{HeaderCheckRecords: 2})
	if err != nil {
		t.Fatalf("NewUploader() error = %v", err)
	}
	spool.Roll(uploadertest.NewSegment().Member(
		uploadertest.RequestPayload("a", 1, map[string]string{"nvcf-ncaid": "other"}),
		uploadertest.RequestPayload("b", 1, map[string]string{"nvcf-ncaid": "other"}),
	).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	var warned []string
	for line := range strings.Lines(logs.String()) {
		if strings.Contains(line, "suppression header") {
			warned = append(warned, line)
		}
	}
	if len(warned) != 1 || !strings.Contains(warned[0], "tenants") || strings.Contains(warned[0], "nca_id") {
		t.Fatalf("warnings = %q, want one naming only the tenants rule", warned)
	}
}

// Suppression counts are recorded once per segment, when it reaches its
// final outcome, not again on every pass that retries it.
func TestSuppressionCountsAreRecordedOncePerSegment(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	spool.Roll(uploadertest.NewSegment().Member(suppressedLine).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvSuppressNCAIDs: "acme"}))
	fake := resubmitSafeFake()
	refused := backendtest.SubmitStep{Outcome: backendtest.Refuse}
	fake.Script(refused, refused)
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := service.NewUploader(cfg, fake, service.Options{Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := uploader.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := metrics.Value(telemetry.RecordsSuppressed, telemetry.Labels{"rule": config.NCAIDRuleName, "action": "strip_payload"}); got != 1 {
		t.Fatalf("records_suppressed = %v after two refusals and a success, want 1", got)
	}
}

// Rejected lines exist only in the source until they are kept, so a source
// whose rejects cannot be kept is not exported or deleted.
func TestASegmentWhoseRejectsCannotBeKeptStays(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1), `{"broken`).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	quarantine := t.TempDir()
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvQuarantineDir: quarantine}))
	// A directory where the kept rejects must go makes publishing them fail
	// after they were written.
	blocker := filepath.Join(quarantine, "rejects", filepath.Base(closed), "x")
	if err := os.MkdirAll(blocker, 0o750); err != nil {
		t.Fatal(err)
	}
	exporter := newCapture(t, resubmitSafeFake())
	uploader, err := service.NewUploader(cfg, exporter, service.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(closed); err != nil {
		t.Fatalf("source after a failed reject commit: %v, want it kept", err)
	}
	if got := exporter.exported(); len(got) != 0 {
		t.Fatalf("exported %q, want nothing until the rejects are kept", got)
	}
}

// A segment that cannot be exported leaves the spool for the quarantine
// directory instead of being read and warned about on every pass.
func TestAQuarantinedSegmentMovesToTheQuarantineDir(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(uploadertest.LegacyAuditRecord("a")).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	quarantine := t.TempDir()
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, map[string]string{config.EnvQuarantineDir: quarantine}))
	uploader, err := service.NewUploader(cfg, resubmitSafeFake(), service.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(closed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source after quarantine: %v, want it moved", err)
	}
	if _, err := os.Stat(filepath.Join(quarantine, filepath.Base(closed))); err != nil {
		t.Fatalf("quarantined copy: %v", err)
	}
}

// A status the core does not know is counted, and the segment is not
// treated as exported.
func TestAnUnknownJobStatusIsCounted(t *testing.T) {
	spool := uploadertest.NewSpool(t, config.DefaultSegmentPrefix)
	closed := spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("a", 1)).Bytes())
	spool.Roll(uploadertest.NewSegment().Member(uploadertest.RequestEnd("x", 1)).Bytes())
	cfg := loadEnv(t, spoolEnv(t, spool.Dir, nil))
	fake := resubmitSafeFake()
	fake.Script(backendtest.SubmitStep{Statuses: []backendtest.StatusStep{{Status: "weird_new_state"}}})
	metrics, err := telemetry.NewMemory("fake")
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := service.NewUploader(cfg, fake, service.Options{Metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}
	if err := uploader.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := metrics.Value(telemetry.JobStatusUnknown, nil); got != 1 {
		t.Fatalf("job_status_unknown = %v, want 1", got)
	}
	if _, err := os.Stat(closed); err != nil {
		t.Fatalf("source after an unknown status: %v, want it kept", err)
	}
}
