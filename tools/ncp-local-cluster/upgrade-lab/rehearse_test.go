// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func rehearsalFixture(t *testing.T) (artifactLock, routeContract, string, string) {
	t.Helper()
	root := t.TempDir()
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadRoute("transitions/umbrella-1.0.4-to-1.1.1.json", "umbrella-1.0.4-to-1.1.1", lock)
	if err != nil {
		t.Fatal(err)
	}
	inputs := filepath.Join(root, "inputs")
	if err := os.Mkdir(inputs, 0700); err != nil {
		t.Fatal(err)
	}
	for n := range lock.Selections {
		for plane, ref := range lock.Selections[n].Stacks {
			inv := inventory{SchemaVersion: 1, Releases: []release{{Plane: plane, Name: "fixture"}}, Artifacts: []artifact{{Name: "fixture"}}}
			inv.Source.Version, inv.Source.Tag, inv.Source.Commit = ref.Version, ref.Tag, ref.Commit
			path := filepath.Join(inputs, plane+"-"+ref.Version+".json")
			if err := writeJSON(path, inv); err != nil {
				t.Fatal(err)
			}
			ref.SHA256, err = fileChecksum(path)
			if err != nil {
				t.Fatal(err)
			}
			lock.Selections[n].Stacks[plane] = ref
		}
		writeReleaseFixtureInputs(t, &lock.Selections[n], inputs)
	}
	tool := filepath.Join(root, "helmfile")
	verifier := filepath.Join(root, "bundle-verifier")
	values, secrets := filepath.Join(root, "values.json"), filepath.Join(root, "secrets.json")
	for _, p := range []string{tool, verifier, values, secrets} {
		if err := os.WriteFile(p, []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	config := rehearsalConfig{SourceRepository: root, Inputs: inputs, Helmfile: tool, BundleVerifier: verifier, ComputeRegistrations: []rehearsalRegistration{{ClusterName: "compute-a", NCAID: "fixture", Values: values}, {ClusterName: "compute-b", NCAID: "fixture", Values: values}}, Overlays: map[string]map[string]rehearsalOverlay{}}
	for _, endpoint := range []string{"source", "target"} {
		config.Overlays[endpoint] = map[string]rehearsalOverlay{}
		for _, plane := range planes {
			config.Overlays[endpoint][plane] = rehearsalOverlay{Values: values, Secrets: secrets}
		}
	}
	configPath := filepath.Join(root, "config.json")
	if err := writeJSON(configPath, config); err != nil {
		t.Fatal(err)
	}
	return lock, route, configPath, filepath.Join(root, "run")
}

func rehearsalExecutor(t *testing.T, lock artifactLock, calls *[]string, failRender, emptyRender bool) executor {
	t.Helper()
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		*calls = append(*calls, name+" "+strings.Join(args, " "))
		if filepath.Base(name) == "bundle-verifier" {
			if len(args) != 8 || args[0] != "umbrella" || args[1] != "verify-bundle" {
				t.Fatalf("incorrect native verifier command: %v", args)
			}
			return nil, nil
		}
		switch name {
		case "git":
			if args[2] == "rev-parse" {
				for _, selected := range lock.Selections {
					if selected.Release != nil && args[4] == "refs/tags/"+selected.Release.Tag+"^{commit}" {
						return []byte(selected.Release.Commit + "\n"), nil
					}
					for _, ref := range selected.Stacks {
						if args[4] == "refs/tags/"+ref.Tag+"^{commit}" {
							return []byte(ref.Commit + "\n"), nil
						}
					}
				}
				t.Fatal("unlocked source tag")
			}
			if err := os.WriteFile(strings.TrimPrefix(args[4], "--output="), []byte(args[5]+" "+args[6]), 0600); err != nil {
				t.Fatal(err)
			}
		case "tar":
			// Source extraction is mocked. Overlay parent directories are created by prepare.
		case "env":
			if !strings.Contains(strings.Join(args, " "), "KUBECONFIG=") || !strings.Contains(strings.Join(args, " "), "--kubeconfig") {
				t.Fatal("render used the default Kubernetes context")
			}
			for _, variable := range []string{"HELM_CACHE_HOME=", "HELM_CONFIG_HOME=", "HELM_DATA_HOME=", "HELM_REPOSITORY_CONFIG=", "HELM_REPOSITORY_CACHE=", "HELMFILE_CACHE_HOME=", "TMPDIR="} {
				if !strings.Contains(strings.Join(args, " "), variable) {
					t.Fatalf("render did not isolate %s", variable)
				}
			}
			if failRender {
				return []byte("private credential fixture"), errors.New("private credential fixture")
			}
			if !emptyRender {
				if err := os.WriteFile(filepath.Join(args[len(args)-1], "manifest.yaml"), []byte("kind: ConfigMap\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
		default:
			t.Fatalf("unexpected command %s", name)
		}
		return nil, nil
	}
}

func TestPrepareRehearsalUsesIndependentImmutableSources(t *testing.T) {
	lock, route, config, out := rehearsalFixture(t)
	var calls []string
	if err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, rehearsalExecutor(t, lock, &calls, false, false)); err != nil {
		t.Fatal(err)
	}
	var record rehearsalRecord
	if err := readJSON(filepath.Join(out, "record.json"), &record); err != nil {
		t.Fatal(err)
	}
	if record.SchemaVersion != 2 || record.Status != "prepared" || record.Qualification != "unqualified" || len(record.Renders) != 12 || len(record.Sources) != 6 || len(record.ReleaseInputs) != 2 || record.BundleVerifierSHA256 == "" || record.FinishedAt == nil {
		t.Fatalf("incomplete rehearsal record: %+v", record)
	}
	for _, source := range record.Sources {
		version := route.From
		if source.Endpoint == "target" {
			version = route.To
		}
		selected, _ := lock.selection(version)
		if source.Ref != selected.Stacks[source.Plane] || source.ArchiveSHA256 == "" || source.ValuesSHA256 == "" || source.SecretsSHA256 == "" {
			t.Fatalf("incorrect source identity: %+v", source)
		}
	}
	info, err := os.Stat(filepath.Join(out, "record.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("rehearsal record is not owner-only")
	}
	if err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, rehearsalExecutor(t, lock, &calls, false, false)); err == nil {
		t.Fatal("overwrote an existing run")
	}
}

func TestPrepareRendersObservabilityForEveryClusterProfile(t *testing.T) {
	lock, route, config, out := rehearsalFixture(t)
	var calls []string
	if err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, rehearsalExecutor(t, lock, &calls, false, false)); err != nil {
		t.Fatal(err)
	}
	var record rehearsalRecord
	if err := readJSON(filepath.Join(out, "record.json"), &record); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{"source", "target"} {
		for _, profile := range []string{"control", "compute/compute-a", "compute/compute-b"} {
			if record.Renders[endpoint+"/observability/"+profile] == "" {
				t.Fatalf("missing %s observability profile %s", endpoint, profile)
			}
		}
	}
	profiles := map[string]int{}
	for _, call := range calls {
		if !strings.HasPrefix(call, "env ") || !strings.Contains(call, "--state-values-set observability.profile=") {
			continue
		}
		for _, profile := range []string{"control", "compute"} {
			if strings.Contains(call, "--state-values-set observability.profile="+profile+" template ") {
				profiles[profile]++
			}
		}
	}
	if profiles["control"] != 2 || profiles["compute"] != 4 {
		t.Fatalf("observability profiles were not explicitly selected: %v", profiles)
	}
}

func TestPrepareRehearsalStopsAtFailedOrEmptyRender(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "empty"}[empty], func(t *testing.T) {
			lock, route, config, out := rehearsalFixture(t)
			var calls []string
			err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, rehearsalExecutor(t, lock, &calls, !empty, empty))
			if err == nil || strings.Contains(err.Error(), "credential") {
				t.Fatalf("incorrect preparation failure: %v", err)
			}
			var record rehearsalRecord
			if err := readJSON(filepath.Join(out, "record.json"), &record); err != nil {
				t.Fatal(err)
			}
			if record.Status != "blocked" || len(record.Renders) != 0 || !strings.Contains(record.NextSafeAction, "Do not apply") {
				t.Fatalf("unsafe failure record: %+v", record)
			}
		})
	}
}

func TestPrepareRejectsMovedTagBeforeArchive(t *testing.T) {
	lock, route, config, out := rehearsalFixture(t)
	calls := 0
	err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return []byte(strings.Repeat("0", 40)), nil
	})
	if err == nil || !strings.Contains(err.Error(), "differs from locked commit") || calls != 1 {
		t.Fatalf("did not stop before source extraction: %v, calls=%d", err, calls)
	}
}

func TestPrepareRejectsChangedInventoryBeforeCommands(t *testing.T) {
	lock, route, config, out := rehearsalFixture(t)
	if err := os.WriteFile(filepath.Join(filepath.Dir(config), "inputs", "control-plane-1.0.10.json"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("executed a command with changed inputs")
		return nil, nil
	}); err == nil {
		t.Fatal("accepted changed input")
	}
}

func TestRenderDigestRejectsEmptyOrSymlinkedOutput(t *testing.T) {
	root := t.TempDir()
	if _, err := renderedTreeChecksum(root); err == nil {
		t.Fatal("accepted an empty render")
	}
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "manifest.yaml")); err != nil {
		t.Fatal(err)
	}
	if _, err := renderedTreeChecksum(root); err == nil {
		t.Fatal("accepted a symlinked render")
	}
}

func TestPrepareWithoutRegistrationsKeepsComputeBlocked(t *testing.T) {
	lock, route, configPath, out := rehearsalFixture(t)
	config, _, err := readRehearsalConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config.ComputeRegistrations = nil
	if err := writeJSON(configPath, config); err != nil {
		t.Fatal(err)
	}
	var calls []string
	err = prepareRehearsal(context.Background(), lock, route, out, configPath, time.Second, rehearsalExecutor(t, lock, &calls, false, false))
	if err == nil || !strings.Contains(err.Error(), "both retained registration files") {
		t.Fatalf("missing registration gate: %v", err)
	}
	var record rehearsalRecord
	if err := readJSON(filepath.Join(out, "record.json"), &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "blocked" || len(record.Renders) != 4 {
		t.Fatalf("incorrect partial preparation: %+v", record)
	}
	for _, call := range calls {
		if strings.HasPrefix(call, "env ") && strings.Contains(call, "/nvcf-compute-plane ") {
			t.Fatal("rendered compute with invented or inherited registration values")
		}
	}
}

func TestRehearsalRegistrationConfiguration(t *testing.T) {
	for _, invalid := range []string{"one", "duplicate", "path", "name", "account"} {
		t.Run(invalid, func(t *testing.T) {
			_, _, path, _ := rehearsalFixture(t)
			config, _, err := readRehearsalConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			switch invalid {
			case "one":
				config.ComputeRegistrations = config.ComputeRegistrations[:1]
			case "duplicate":
				config.ComputeRegistrations[1].ClusterName = config.ComputeRegistrations[0].ClusterName
			case "path":
				config.ComputeRegistrations[1].Values = "relative"
			case "name":
				config.ComputeRegistrations[1].ClusterName = "../other"
			case "account":
				config.ComputeRegistrations[1].NCAID = ""
			}
			if err := writeJSON(path, config); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readRehearsalConfig(path); err == nil {
				t.Fatal("accepted invalid registration inputs")
			}
		})
	}
}
