// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCommittedContracts(t *testing.T) {
	root := "transitions"
	lock, err := loadLock(filepath.Join(root, "artifact-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "*-to-*.json"))
	if err != nil || len(files) != 2 {
		t.Fatalf("contracts: %v %v", files, err)
	}
	for _, path := range files {
		id := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(id, func(t *testing.T) {
			r, err := loadRoute(path, id, lock)
			if err != nil {
				t.Fatal(err)
			}
			if r.From == r.To {
				t.Fatal("route must change umbrella endpoint identity")
			}
			var execution struct {
				Enabled *bool `json:"supported_execution_enabled"`
			}
			if err := readJSON(path, &execution); err != nil {
				t.Fatal(err)
			}
			if execution.Enabled == nil || *execution.Enabled {
				t.Fatal("committed routes must explicitly disable supported execution")
			}
		})
	}
}

func TestPeerSelectionRequiresItsEndpointMatrix(t *testing.T) {
	for _, plane := range []string{"compute-plane", "observability", "missing-matrix"} {
		t.Run(plane, func(t *testing.T) {
			lock, err := loadLock(filepath.Join("transitions", "artifact-lock.json"))
			if err != nil {
				t.Fatal(err)
			}
			target := &lock.Selections[len(lock.Selections)-1]
			if plane == "missing-matrix" {
				target.PeerMatrixEdition = "unknown"
			} else {
				// A valid earlier peer identity must not replace the target matrix's peer.
				target.Stacks[plane] = lock.Selections[0].Stacks[plane]
			}
			path := filepath.Join(t.TempDir(), "artifact-lock.json")
			if err := writeLockFixture(path, lock); err != nil {
				t.Fatal(err)
			}
			if _, err := loadLock(path); err == nil {
				t.Fatal("accepted a missing matrix or a peer selection from another endpoint")
			}
		})
	}
}

func TestInventoryDigestAndIdentity(t *testing.T) {
	inv := inventory{SchemaVersion: 1, Releases: []release{{Plane: "control-plane", Name: "service"}}, Artifacts: []artifact{{Name: "service"}}}
	inv.Source.Version = "1.2.3"
	inv.Source.Tag = "deploy/stacks/self-managed/v1.2.3"
	inv.Source.Commit = strings.Repeat("a", 40)
	raw, _ := json.Marshal(inv)
	ref := stackRef{Version: inv.Source.Version, Tag: inv.Source.Tag, Commit: inv.Source.Commit, SHA256: checksum(raw)}
	if _, err := verifyInventory(raw, ref, "control-plane"); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyInventory(append(raw, ' '), ref, "control-plane"); err == nil {
		t.Fatal("accepted digest drift")
	}
	ref.Commit = strings.Repeat("b", 40)
	if _, err := verifyInventory(raw, ref, "control-plane"); err == nil {
		t.Fatal("accepted wrong source commit")
	}
	ref.Commit = inv.Source.Commit
	if _, err := verifyInventory(raw, ref, "compute-plane"); err == nil {
		t.Fatal("accepted wrong deployment plane")
	}
}

func TestConsumerAndMultipleVersionComparison(t *testing.T) {
	makeArtifact := func(consumer, version string) artifact {
		return artifact{Type: "container-image", Name: "migration", Version: version, Reference: "registry.invalid/migration:" + version, Sources: []artifactSource{{Plane: "control-plane", Release: consumer, Requirement: "required"}}}
	}
	from := map[string]inventory{"control-plane": {Artifacts: []artifact{makeArtifact("openbao", "1.0.0"), makeArtifact("ui", "0.9.0")}}}
	to := map[string]inventory{"control-plane": {Artifacts: []artifact{makeArtifact("openbao", "1.1.0"), makeArtifact("ui", "0.9.0"), makeArtifact("router", "1.0.0"), makeArtifact("router", "1.1.0")}}}
	lock := artifactLock{Selections: []selection{{Version: "a"}, {Version: "b"}}}
	r, err := compareRoute(lock, routeContract{From: "a", To: "b"}, map[string]map[string]inventory{"a": from, "b": to})
	if err != nil {
		t.Fatal(err)
	}
	if r.Summary.Changed != 1 || r.Summary.Added != 2 || r.Summary.Backward != 0 {
		t.Fatalf("cross-consumer or variant comparison lost: %+v", r.Summary)
	}
	if len(r.Components) != 4 {
		t.Fatalf("lost component variants: %+v", r.Components)
	}
}

func TestSharedReferencesSurviveVariantCountChanges(t *testing.T) {
	makeInventory := func(versions ...string) map[string]inventory {
		inv := inventory{}
		for _, version := range versions {
			inv.Artifacts = append(inv.Artifacts, artifact{Type: "container-image", Name: "helper", Version: version, Reference: "registry.invalid/helper:" + version, Sources: []artifactSource{{Plane: "control-plane", Release: "service"}}})
		}
		return map[string]inventory{"control-plane": inv}
	}
	for _, tc := range []struct {
		name     string
		from, to []string
		added    int
		removed  int
	}{
		{"one-to-many", []string{"1.0.0"}, []string{"1.0.0", "2.0.0"}, 1, 0},
		{"many-to-one", []string{"1.0.0", "2.0.0"}, []string{"1.0.0"}, 0, 1},
		{"many-to-many", []string{"1.0.0", "2.0.0"}, []string{"1.0.0", "3.0.0"}, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lock := artifactLock{Selections: []selection{{Version: "a"}, {Version: "b"}}}
			r, err := compareRoute(lock, routeContract{From: "a", To: "b"}, map[string]map[string]inventory{"a": makeInventory(tc.from...), "b": makeInventory(tc.to...)})
			if err != nil {
				t.Fatal(err)
			}
			if r.Summary.Added != tc.added || r.Summary.Removed != tc.removed || r.Summary.Changed != 0 {
				t.Fatalf("variant count changes fabricated a replacement: %+v", r.Summary)
			}
			shared := 0
			for _, c := range r.Components {
				if c.Change == "unchanged" && c.From == "registry.invalid/helper:1.0.0" && c.To == c.From {
					shared++
				}
			}
			if shared != 1 {
				t.Fatal("lost the shared reference")
			}
		})
	}
}

func TestLockRejectsConflictingSharedInventories(t *testing.T) {
	for _, field := range []string{"unchanged", "commit", "digest", "asset-id"} {
		t.Run(field, func(t *testing.T) {
			lock, err := loadLock("transitions/artifact-lock.json")
			if err != nil {
				t.Fatal(err)
			}
			ref := lock.Selections[1].Stacks["observability"]
			switch field {
			case "commit":
				ref.Commit = strings.Repeat("a", 40)
			case "digest":
				ref.SHA256 = "sha256:" + strings.Repeat("a", 64)
			case "asset-id":
				ref.AssetID++
			}
			lock.Selections[1].Stacks["observability"] = ref
			root := t.TempDir()
			path := filepath.Join(root, "artifact-lock.json")
			if err := writeLockFixture(path, lock); err != nil {
				t.Fatal(err)
			}
			_, err = loadLock(path)
			if field == "unchanged" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "conflicting inventory") {
				t.Fatalf("accepted conflicting %s for a shared inventory: %v", field, err)
			}
			out := filepath.Join(root, "must-not-fetch")
			err = run(context.Background(), []string{"fetch", "--contracts", root, "--route", "umbrella-1.0.1-to-1.0.4", "--out", out})
			if err == nil || !strings.Contains(err.Error(), "conflicting inventory") {
				t.Fatalf("fetch did not reject the conflicting lock first: %v", err)
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("fetch created output before rejecting the conflicting lock")
			}
		})
	}
}

func TestRouteRejectsUnchangedEndpoint(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	var route routeContract
	if err := readJSON("transitions/umbrella-1.0.4-to-1.1.1.json", &route); err != nil {
		t.Fatal(err)
	}
	route.To = route.From
	route.ID = "umbrella-" + route.From + "-to-" + route.To
	path := filepath.Join(t.TempDir(), "route.json")
	if err := writeJSON(path, route); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRoute(path, route.ID, lock); err == nil {
		t.Fatal("accepted a route with identical endpoints")
	}
}

func TestOptionalDowngradeRemainsAConditionalFinding(t *testing.T) {
	root := "transitions"
	lock, err := loadLock(filepath.Join(root, "artifact-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	id := "umbrella-1.0.4-to-1.1.1"
	r, err := loadRoute(filepath.Join(root, id+".json"), id, lock)
	if err != nil {
		t.Fatal(err)
	}
	r.ConditionalChecks = []conditionalCheck{{
		ID:                "optional-artifact-downgrade",
		AppliesWhen:       "An enabled optional component selects a lower version.",
		WhenApplicable:    "Review the actual artifact changes before the action.",
		WhenNotApplicable: "Retain disabled values at both endpoints; this check does not block the profile.",
		WhenUnknown:       "The audit does not evaluate applicability.",
		Evidence:          []string{"effective values and renders"},
	}}
	makeInventory := func(version string) map[string]inventory {
		return map[string]inventory{"compute-plane": {Artifacts: []artifact{{Type: "container-image", Name: "nats-server-config-reloader", Version: version, Reference: "registry.invalid/reloader:" + version, Sources: []artifactSource{{Plane: "compute-plane", Release: "dynamo-operator", Requirement: "optional"}}}}}}
	}
	report, err := compareRoute(lock, r, map[string]map[string]inventory{r.From: makeInventory("0.25.0"), r.To: makeInventory("0.24.0")})
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary.Backward != 1 || len(report.Components) != 1 || !strings.Contains(report.Components[0].Review, "enabled/rendered use") {
		t.Fatalf("lost conditional inventory downgrade: %+v", report)
	}
	for _, blocker := range report.ExecutionBlockers {
		if strings.Contains(blocker, "reloader") {
			t.Fatal("optional Dynamo reloader became an unconditional blocker")
		}
	}
	found := false
	for _, c := range report.ConditionalChecks {
		if c.ID == "optional-artifact-downgrade" {
			found = strings.Contains(c.WhenNotApplicable, "disabled values at both endpoints") && strings.Contains(c.WhenUnknown, "audit does not evaluate applicability")
		}
	}
	markdown := renderComparison(report)
	if !found || !strings.Contains(markdown, "## Conditional checks") || !strings.Contains(markdown, "this check does not block the profile") {
		t.Fatal("report omitted applicability, non-blocking absence evidence or the unknown-state limitation")
	}
}

func TestConditionalChecksRejectMissingDecisionsAndEvidence(t *testing.T) {
	for _, missing := range []string{"not-applicable", "unknown", "evidence", "duplicate"} {
		t.Run(missing, func(t *testing.T) {
			root, id := "transitions", "umbrella-1.0.4-to-1.1.1"
			lock, err := loadLock(filepath.Join(root, "artifact-lock.json"))
			if err != nil {
				t.Fatal(err)
			}
			r, err := loadRoute(filepath.Join(root, id+".json"), id, lock)
			if err != nil {
				t.Fatal(err)
			}
			switch missing {
			case "not-applicable":
				r.ConditionalChecks[0].WhenNotApplicable = ""
			case "unknown":
				r.ConditionalChecks[0].WhenUnknown = ""
			case "evidence":
				r.ConditionalChecks[0].Evidence = nil
			case "duplicate":
				r.ConditionalChecks = append(r.ConditionalChecks, r.ConditionalChecks[0])
			}
			path := filepath.Join(t.TempDir(), "route.json")
			if err := writeJSON(path, r); err != nil {
				t.Fatal(err)
			}
			if _, err := loadRoute(path, id, lock); err == nil || !strings.Contains(err.Error(), "conditional check") {
				t.Fatalf("accepted invalid conditional check: %v", err)
			}
		})
	}
}

func TestVersionOrderingDoesNotGuessSuffixes(t *testing.T) {
	for _, tc := range []struct {
		a, b  string
		want  int
		known bool
	}{
		{"1.0.0", "1.0.1", -1, true}, {"2.6.3-nv-1.3.9", "2.6.3-nv-1.3.7", 1, true},
		{"v1.20.5", "v1.20.2", 1, true}, {"2.14.9-alpine3.22", "2.14.6-alpine3.22", 1, true},
		{"1.0.0-alpha", "1.0.0-beta", 0, false}, {"arbitrary", "another", 0, false},
		{"2.14.6-alpine3.22", "2.14.9-alpine3.23", 0, false},
		{"2.14.9-alpine3.23", "2.14.6-alpine3.22", 0, false},
		{"1.0.0-alpha", "2.0.0", 0, false},
		{"2.6.2-nv-1.3.4", "2.6.3-nv-1.3.7", -1, true},
		{"2.6.2-nv-unknown", "2.6.3-nv-other", 0, false},
	} {
		got, ok := compareVersion(tc.a, tc.b)
		if got != tc.want || ok != tc.known {
			t.Errorf("%s %s: %d %v", tc.a, tc.b, got, ok)
		}
	}
}

func TestProbeFailsClosedAndKeepsRawOutputPrivate(t *testing.T) {
	t.Setenv("UPGRADE_LAB_HELM_REPOSITORY_CONFIG", "")
	inv := inventory{Releases: []release{{Name: "service", Required: true}}, Artifacts: []artifact{{Type: "container-image", Name: "service", Reference: "registry.invalid/service:1.0.0", Sources: []artifactSource{{Plane: "control-plane", Release: "service"}}}}}
	inputs := map[string]map[string]inventory{"a": {"control-plane": inv}, "b": {"control-plane": inv}}
	out := t.TempDir()
	calls := 0
	err := probeRoute(context.Background(), routeContract{From: "a", To: "b"}, inputs, out, time.Second, func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return []byte("secret-output"), errors.New("secret-error")
	})
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe or missing failure: %v", err)
	}
	if calls != 1 {
		t.Fatal("duplicate immutable reference was reprobed")
	}
	var report accessReport
	if err := readJSON(filepath.Join(out, "access.json"), &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "blocked" || !report.Complete || !report.Results[0].Required {
		t.Fatalf("fail-open report: %+v", report)
	}
	raw, err := os.ReadFile(filepath.Join(out, report.Results[0].RawOutput))
	if err != nil || string(raw) != "secret-output" {
		t.Fatalf("missing private diagnostics: %v", err)
	}
	st, _ := os.Stat(filepath.Join(out, report.Results[0].RawOutput))
	if st.Mode().Perm() != 0600 {
		t.Fatal("raw diagnostics are not private")
	}
}

func TestProbeRejectsUnknownPlatformAndCancellation(t *testing.T) {
	if platformDigest([]byte(`{"digest":"sha256:`+strings.Repeat("a", 64)+`","platform":{"os":"linux","architecture":"arm64"}}`)) != "" {
		t.Fatal("accepted wrong architecture")
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	if got := platformDigest([]byte(`[{"Descriptor":{"digest":"` + digest + `","platform":{"os":"linux","architecture":"amd64"}}}]`)); got != digest {
		t.Fatal(got)
	}
	inv := inventory{Artifacts: []artifact{{Type: "container-image", Name: "helper", Reference: "registry.invalid/helper:fixture", Sources: []artifactSource{{Plane: "control-plane", Release: "service", Requirement: "required"}}}}}
	inputs := map[string]map[string]inventory{"a": {"control-plane": inv}, "b": {"control-plane": inv}}
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "canceled"}[canceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			calls, out := 0, t.TempDir()
			err := probeRoute(ctx, routeContract{From: "a", To: "b"}, inputs, out, time.Second, func(context.Context, string, ...string) ([]byte, error) {
				calls++
				return []byte(`{"digest":"` + digest + `","platform":{"os":"linux","architecture":"amd64"}}`), nil
			})
			var report accessReport
			if readErr := readJSON(filepath.Join(out, "access.json"), &report); readErr != nil {
				t.Fatal(readErr)
			}
			if canceled {
				if err == nil || calls != 0 || report.Status != "blocked" || report.Complete {
					t.Fatalf("canceled probe executed or passed: calls=%d, report=%+v, err=%v", calls, report, err)
				}
			} else if err != nil || calls != 1 || report.Status != "passed" || !report.Complete {
				t.Fatalf("valid probe did not pass: calls=%d, report=%+v, err=%v", calls, report, err)
			}
		})
	}
}

func TestRouteRejectsForwardDependencies(t *testing.T) {
	root := "transitions"
	lock, err := loadLock(filepath.Join(root, "artifact-lock.json"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "umbrella-1.0.1-to-1.0.4.json")
	var r routeContract
	if err := readJSON(path, &r); err != nil {
		t.Fatal(err)
	}
	r.Actions[0].Needs = []string{r.Actions[len(r.Actions)-1].ID}
	bad := filepath.Join(t.TempDir(), "route.json")
	if err := writeJSON(bad, r); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRoute(bad, r.ID, lock); err == nil {
		t.Fatal("accepted dependency cycle")
	}
	if _, err := loadRoute(filepath.Join(t.TempDir(), "absent.json"), "../route", lock); err == nil || !strings.Contains(err.Error(), "provide an umbrella route ID") {
		t.Fatalf("route ID was not rejected before file access: %v", err)
	}
}

func TestSourceChartsRequireTheirOwningCommit(t *testing.T) {
	commit := strings.Repeat("a", 40)
	inv := inventory{}
	inv.Source.Commit = commit
	in := map[string]map[string]inventory{"test": {"observability": inv}}
	repo := "https://github.com/NVIDIA/nvcf/tree/" + commit + "/deploy/stacks/observability/charts"
	if _, _, err := sourceChartLocation(repo, "nvcf-default-monitors", in); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(repo, commit, strings.Repeat("b", 40), 1), strings.Replace(repo, "observability", "self-managed", 1), strings.Replace(repo, "NVIDIA/nvcf", "unexpected/repo", 1)} {
		if _, _, err := sourceChartLocation(bad, "nvcf-default-monitors", in); err == nil {
			t.Fatal("accepted unowned source chart")
		}
	}
	if _, _, err := sourceChartLocation(repo, "../escape", in); err == nil {
		t.Fatal("accepted source chart path traversal")
	}
}

func TestSourceDataGatePrecedesBackupAndHop(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"umbrella-1.0.1-to-1.0.4", "umbrella-1.0.4-to-1.1.1"} {
		t.Run(id, func(t *testing.T) {
			route, err := loadRoute("transitions/"+id+".json", id, lock)
			if err != nil {
				t.Fatal(err)
			}
			ancestors := map[string]map[string]bool{}
			var dataGate action
			for _, current := range route.Actions {
				ancestors[current.ID] = map[string]bool{}
				for _, dependency := range current.Needs {
					ancestors[current.ID][dependency] = true
					for ancestor := range ancestors[dependency] {
						ancestors[current.ID][ancestor] = true
					}
				}
				if current.ID == "source-data-prerequisites" {
					dataGate = current
				}
				if (current.ID == "backup" || current.Phase == "during-hop") && !ancestors[current.ID]["source-data-prerequisites"] {
					t.Fatalf("%s can start without the source data gate", current.ID)
				}
				if (current.ID == "source-data-prerequisites" || current.ID == "backup") && !ancestors[current.ID]["drain"] {
					t.Fatalf("%s can start before callers and source writers pause", current.ID)
				}
				if current.Phase == "during-hop" && !ancestors[current.ID]["backup"] {
					t.Fatalf("%s can start before recovery is verified", current.ID)
				}
			}
			if dataGate.Phase != "before-hop" || dataGate.Owner == "" || len(dataGate.Postconditions) == 0 || len(dataGate.Evidence) == 0 {
				t.Fatal("missing source data completion gate")
			}
		})
	}
}

func TestRuntimeArtifactChecksRemainSeparateFromInventory(t *testing.T) {
	lock := artifactLock{Selections: []selection{{Version: "a"}, {Version: "b"}}}
	route := routeContract{From: "a", To: "b", RuntimeArtifactChecks: []string{"Resolve workload-created helpers"}}
	report, err := compareRoute(lock, route, map[string]map[string]inventory{"a": {}, "b": {}})
	if err != nil {
		t.Fatal(err)
	}
	if report.ComparisonScope != "release-inventories" || report.Summary.Qualification != "unqualified" || len(report.RuntimeArtifactChecks) != 1 {
		t.Fatalf("lost inventory/runtime boundary: %+v", report)
	}
	markdown := renderComparison(report)
	if !strings.Contains(markdown, "They do not establish runtime absence") || !strings.Contains(markdown, route.RuntimeArtifactChecks[0]) {
		t.Fatal("report omitted the runtime artifact qualification boundary")
	}
}

func TestRejectBlankRuntimeArtifactCheck(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	id := "umbrella-1.0.1-to-1.0.4"
	route, err := loadRoute("transitions/"+id+".json", id, lock)
	if err != nil {
		t.Fatal(err)
	}
	route.RuntimeArtifactChecks = []string{" "}
	path := filepath.Join(t.TempDir(), id+".json")
	if err := writeJSON(path, route); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRoute(path, id, lock); err == nil || !strings.Contains(err.Error(), "runtime artifact") {
		t.Fatalf("accepted a blank runtime artifact gate: %v", err)
	}
}
