// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeLockFixture(path string, lock artifactLock) error {
	for _, selected := range lock.Selections {
		if selected.Manifest == nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("transitions", selected.Manifest.Path))
		if err != nil {
			return err
		}
		destination := filepath.Join(filepath.Dir(path), selected.Manifest.Path)
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(destination, raw, 0600); err != nil {
			return err
		}
	}
	return writeJSON(path, lock)
}

func TestUmbrellaSelectionsResolveActualStacks(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := lock.selection("1.0.4")
	if err != nil {
		t.Fatal(err)
	}
	if selected.Kind != "umbrella-release" || selected.Stacks["control-plane"].Version != "1.0.10" {
		t.Fatal("umbrella 1.0.4 must select stack 1.0.10")
	}
	legacy, _ := lock.selection("1.0.1")
	if legacy.Kind != "legacy-baseline" || legacy.Manifest != nil {
		t.Fatal("legacy baseline must not claim a published umbrella manifest")
	}
}

func TestUmbrellaLockRejectsChangedInputs(t *testing.T) {
	for _, field := range []string{"schema", "kind", "missing-manifest", "manifest-commit", "manifest-path", "manifest-bytes", "manifest-version", "manifest-stack", "manifest-peers", "legacy-manifest", "legacy-kind"} {
		t.Run(field, func(t *testing.T) {
			lock, err := loadLock("transitions/artifact-lock.json")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "artifact-lock.json")
			if err := writeLockFixture(path, lock); err != nil {
				t.Fatal(err)
			}
			selected := &lock.Selections[1]
			switch field {
			case "schema":
				lock.SchemaVersion = 1
			case "kind":
				selected.Kind = "self-managed-stack"
			case "missing-manifest":
				selected.Manifest = nil
			case "manifest-commit":
				selected.Manifest.SourceCommit = "main"
			case "manifest-path":
				selected.Manifest.Path = "../manifest.json"
			case "legacy-manifest":
				lock.Selections[0].Manifest = selected.Manifest
			case "legacy-kind":
				lock.Selections[0].Kind = "umbrella-release"
			default:
				manifestPath := filepath.Join(filepath.Dir(path), selected.Manifest.Path)
				raw, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				var manifest umbrellaManifest
				if err := json.Unmarshal(raw, &manifest); err != nil {
					t.Fatal(err)
				}
				switch field {
				case "manifest-bytes":
					raw = append(raw, ' ')
				case "manifest-version":
					manifest.Version = "9.9.9"
				case "manifest-stack":
					changed := manifest.Stacks["control-plane"]
					changed.Commit = strings.Repeat("a", 40)
					manifest.Stacks["control-plane"] = changed
				case "manifest-peers":
					changed := manifest.Stacks["control-plane"]
					changed.Compatible["compute-plane"] = "9.9.9"
					manifest.Stacks["control-plane"] = changed
				}
				if field != "manifest-bytes" {
					raw, err = json.Marshal(manifest)
					if err != nil {
						t.Fatal(err)
					}
					selected.Manifest.SHA256 = checksum(raw)
				}
				if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := writeJSON(path, lock); err != nil {
				t.Fatal(err)
			}
			if _, err := loadLock(path); err == nil {
				t.Fatalf("accepted changed %s", field)
			}
		})
	}
}

func TestRouteRejectsOldScopeAndEnabledExecution(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	id := "umbrella-1.0.1-to-1.0.4"
	for _, field := range []string{"schema", "scope", "enabled", "missing-enabled", "old-id"} {
		t.Run(field, func(t *testing.T) {
			var route routeContract
			if err := readJSON("transitions/"+id+".json", &route); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "schema":
				route.SchemaVersion = 1
			case "scope":
				route.VersionScope = "self-managed-stack"
			case "enabled":
				value := true
				route.SupportedExecutionEnabled = &value
			case "missing-enabled":
				route.SupportedExecutionEnabled = nil
			case "old-id":
				route.ID = "1.0.1-to-1.0.4"
			}
			path := filepath.Join(t.TempDir(), "route.json")
			if err := writeJSON(path, route); err != nil {
				t.Fatal(err)
			}
			if _, err := loadRoute(path, route.ID, lock); err == nil {
				t.Fatalf("accepted changed %s", field)
			}
		})
	}
}
