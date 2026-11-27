// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeReleaseFixtureInputs(t *testing.T, s *selection, inputs string) {
	t.Helper()
	if s.Release == nil {
		return
	}
	dir := releaseInputDir(inputs, *s)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	bundle := []byte("fixture product bundle " + s.Version)
	if err := os.WriteFile(filepath.Join(dir, s.Release.Bundle.Name), bundle, 0600); err != nil {
		t.Fatal(err)
	}
	s.Release.Bundle.SHA256 = checksum(bundle)
	manifest := umbrellaManifest{SchemaVersion: 2, Version: s.Version, SourceCommit: strings.Repeat("a", 40), Stacks: map[string]manifestStack{}}
	assets := map[string]string{s.Release.Bundle.Name: s.Release.Bundle.SHA256}
	for plane, ref := range s.Stacks {
		peers := map[string]string{}
		for peer, other := range s.Stacks {
			if peer != plane {
				peers[peer] = other.Version
			}
		}
		manifest.Stacks[plane] = manifestStack{stackRef: ref, Compatible: peers}
		assets[ref.Asset] = ref.SHA256
	}
	path := filepath.Join(dir, s.Release.Manifest.Name)
	if err := writeJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	var err error
	if s.Release.Manifest.SHA256, err = fileChecksum(path); err != nil {
		t.Fatal(err)
	}
	s.Manifest.SHA256 = s.Release.Manifest.SHA256
	assets[s.Release.Manifest.Name] = s.Release.Manifest.SHA256
	path = filepath.Join(dir, s.Release.AssetIndex.Name)
	if err := writeJSON(path, map[string]any{"schema_version": 1, "version": s.Version, "assets": assets}); err != nil {
		t.Fatal(err)
	}
	if s.Release.AssetIndex.SHA256, err = fileChecksum(path); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseEndpointsKeepProductAndStackVersionsSeparate(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	legacy, product := lock.Selections[0], lock.Selections[1]
	if legacy.Kind != "legacy-baseline" || legacy.Manifest != nil || legacy.Release != nil {
		t.Fatal("legacy baseline acquired a synthetic umbrella identity")
	}
	if product.Kind != "umbrella-release" || product.Version == product.Stacks["control-plane"].Version {
		t.Fatal("product endpoint collapsed into its control-plane stack")
	}
	ref := product.Stacks["control-plane"]
	ref.Version, ref.Tag = product.Version, "deploy/stacks/self-managed/v"+product.Version
	lock.Selections[1].Stacks["control-plane"] = ref
	path := filepath.Join(t.TempDir(), "lock.json")
	if err := writeJSON(path, lock); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLock(path); err == nil || !strings.Contains(err.Error(), "differs from docs matrix") {
		t.Fatalf("accepted a same-numbered stack in place of the selected product stack: %v", err)
	}
}

func TestProductInputsRejectCorruptionAndWrongSelection(t *testing.T) {
	for _, changed := range []string{"manifest-bytes", "bundle-bytes", "index-bytes", "manifest-selection", "index-inventory"} {
		t.Run(changed, func(t *testing.T) {
			lock, route, configPath, _ := rehearsalFixture(t)
			config, _, err := readRehearsalConfig(configPath)
			if err != nil {
				t.Fatal(err)
			}
			s := &lock.Selections[1]
			if err := verifyReleaseInputs(*s, config.Inputs); err != nil {
				t.Fatal(err)
			}
			dir := releaseInputDir(config.Inputs, *s)
			switch changed {
			case "manifest-selection":
				path := filepath.Join(dir, s.Release.Manifest.Name)
				var manifest umbrellaManifest
				if err := readJSON(path, &manifest); err != nil {
					t.Fatal(err)
				}
				ref := manifest.Stacks["control-plane"]
				ref.Version = s.Version
				manifest.Stacks["control-plane"] = ref
				if err := writeJSON(path, manifest); err != nil {
					t.Fatal(err)
				}
				s.Release.Manifest.SHA256, err = fileChecksum(path)
			case "index-inventory":
				path := filepath.Join(dir, s.Release.AssetIndex.Name)
				var index struct {
					SchemaVersion int               `json:"schema_version"`
					Version       string            `json:"version"`
					Assets        map[string]string `json:"assets"`
				}
				if err := readJSON(path, &index); err != nil {
					t.Fatal(err)
				}
				index.Assets[s.Stacks["control-plane"].Asset] = checksum([]byte("another inventory"))
				if err := writeJSON(path, index); err != nil {
					t.Fatal(err)
				}
				s.Release.AssetIndex.SHA256, err = fileChecksum(path)
			default:
				asset := map[string]releaseAsset{"manifest-bytes": s.Release.Manifest, "bundle-bytes": s.Release.Bundle, "index-bytes": s.Release.AssetIndex}[changed]
				err = os.WriteFile(filepath.Join(dir, asset.Name), []byte("changed"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := loadInputs(lock, route, config.Inputs); err == nil {
				t.Fatal("accepted changed product inputs")
			}
		})
	}
}

func TestOldStackSchemasAreNotReleaseEndpoints(t *testing.T) {
	lock, err := loadLock("transitions/artifact-lock.json")
	if err != nil {
		t.Fatal(err)
	}
	route, err := loadRoute("transitions/umbrella-1.0.1-to-1.0.4.json", "umbrella-1.0.1-to-1.0.4", lock)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	lock.SchemaVersion, lock.VersionScope = 1, "self-managed-stack"
	if err := writeJSON(filepath.Join(root, "lock.json"), lock); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLock(filepath.Join(root, "lock.json")); err == nil {
		t.Fatal("accepted a legacy stack lock as a release lock")
	}
	route.SchemaVersion, route.VersionScope = 1, "self-managed-stack"
	if err := writeJSON(filepath.Join(root, "route.json"), route); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRoute(filepath.Join(root, "route.json"), route.ID, lock); err == nil {
		t.Fatal("accepted an old same-numbered stack route")
	}
}

func TestPrepareRejectsBundleBeforeExtraction(t *testing.T) {
	lock, route, config, out := rehearsalFixture(t)
	var calls []string
	exec := rehearsalExecutor(t, lock, &calls, false, false)
	err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if filepath.Base(name) == "bundle-verifier" {
			return nil, errors.New("invalid archive contents")
		}
		return exec(ctx, name, args...)
	})
	if err == nil || !strings.Contains(err.Error(), "verify source umbrella bundle failed") {
		t.Fatalf("native bundle validation did not block extraction: %v", err)
	}
	for _, call := range calls {
		if strings.HasPrefix(call, "tar ") || strings.HasPrefix(call, "env ") {
			t.Fatal("extracted or rendered an unverified bundle")
		}
	}
}

func TestPrepareKeepsLegacyArchivesSeparateFromProductBundles(t *testing.T) {
	lock, _, config, out := rehearsalFixture(t)
	route, err := loadRoute("transitions/umbrella-1.0.1-to-1.0.4.json", "umbrella-1.0.1-to-1.0.4", lock)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	if err := prepareRehearsal(context.Background(), lock, route, out, config, time.Second, rehearsalExecutor(t, lock, &calls, false, false)); err != nil {
		t.Fatal(err)
	}
	var record rehearsalRecord
	if err := readJSON(filepath.Join(out, "record.json"), &record); err != nil {
		t.Fatal(err)
	}
	if len(record.ReleaseInputs) != 1 || record.ReleaseInputs["target"].Bundle.SHA256 == "" {
		t.Fatal("legacy endpoint acquired a synthetic bundle or target lost its original bundle")
	}
	archives := 0
	for _, call := range calls {
		if strings.Contains(call, " archive ") {
			archives++
		}
	}
	if archives != len(planes) {
		t.Fatalf("expected only the legacy stack archives, got %d", archives)
	}
}

func TestFetchBindsOriginalReleaseAssetsBeforeInventories(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			lock, route, configPath, _ := rehearsalFixture(t)
			config, _, err := readRehearsalConfig(configPath)
			if err != nil {
				t.Fatal(err)
			}
			responses := map[string][]byte{}
			inventoryRequests := map[string]bool{}
			for _, version := range []string{route.From, route.To} {
				selected, _ := lock.selection(version)
				dir := releaseInputDir(config.Inputs, selected)
				for _, asset := range []releaseAsset{selected.Release.Manifest, selected.Release.AssetIndex, selected.Release.Bundle} {
					raw, err := os.ReadFile(filepath.Join(dir, asset.Name))
					if err != nil {
						t.Fatal(err)
					}
					responses[fmt.Sprintf("repos/%s/releases/assets/%d", lock.Repository, asset.AssetID)] = raw
				}
				manifest, err := os.ReadFile(filepath.Join(dir, selected.Release.Manifest.Name))
				if err != nil {
					t.Fatal(err)
				}
				responses[fmt.Sprintf("repos/%s/contents/deploy/releases/%s/manifest.json?ref=%s", lock.Repository, version, selected.Manifest.SourceCommit)] = manifest
				for plane, ref := range selected.Stacks {
					path := fmt.Sprintf("repos/%s/releases/assets/%d", lock.Repository, ref.AssetID)
					raw, err := os.ReadFile(filepath.Join(config.Inputs, inventoryName(plane, ref)))
					if err != nil {
						t.Fatal(err)
					}
					responses[path], inventoryRequests[path] = raw, true
				}
			}
			if corrupt {
				asset := lock.Selections[1].Release.AssetIndex
				responses[fmt.Sprintf("repos/%s/releases/assets/%d", lock.Repository, asset.AssetID)] = []byte("changed asset index")
			}
			inventoryCalls := 0
			output := t.TempDir()
			err = fetchInputsWithExecutor(context.Background(), lock, route, output, time.Second, func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "gh" {
					t.Fatalf("unexpected command: %s", name)
				}
				path := args[len(args)-1]
				if inventoryRequests[path] {
					inventoryCalls++
				}
				raw, ok := responses[path]
				if !ok {
					t.Fatalf("unlocked request: %s", path)
				}
				return raw, nil
			})
			if corrupt {
				if err == nil || !strings.Contains(err.Error(), "release asset digest mismatch") || inventoryCalls != 0 {
					t.Fatalf("corrupt release assets reached inventories: calls=%d err=%v", inventoryCalls, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := loadInputs(lock, route, output); err != nil {
					t.Fatal(err)
				}
				if inventoryCalls != len(inventoryRequests) {
					t.Fatal("did not fetch every distinct original inventory")
				}
			}
		})
	}
}
