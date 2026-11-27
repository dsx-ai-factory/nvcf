// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type artifactLock struct {
	SHA256                string                `json:"-"`
	SchemaVersion         int                   `json:"schema_version"`
	VersionScope          string                `json:"version_scope"`
	Repository            string                `json:"repository"`
	Selections            []selection           `json:"selections"`
	DocumentationMatrices []documentationMatrix `json:"documentation_matrices"`
}
type documentationMatrix struct {
	Edition      string            `json:"edition"`
	URL          string            `json:"url"`
	SourceCommit string            `json:"source_commit"`
	Stacks       map[string]string `json:"documented_stacks"`
}
type selection struct {
	Version           string              `json:"version"`
	Kind              string              `json:"kind"`
	Stacks            map[string]stackRef `json:"stacks"`
	PeerMatrixEdition string              `json:"peer_matrix_edition"`
	Manifest          *manifestRef        `json:"manifest,omitempty"`
	Release           *releaseRef         `json:"release,omitempty"`
}
type stackRef struct {
	Version string `json:"version"`
	Tag     string `json:"source_tag"`
	Commit  string `json:"source_commit"`
	Asset   string `json:"inventory_asset"`
	SHA256  string `json:"inventory_sha256"`
	AssetID int64  `json:"inventory_asset_id"`
}
type inventory struct {
	SchemaVersion int                                   `json:"schema_version"`
	Source        struct{ Version, Tag, Commit string } `json:"source"`
	Releases      []release                             `json:"releases"`
	Artifacts     []artifact                            `json:"artifacts"`
}
type release struct {
	Plane, Name, Namespace, Chart, Version string
	Required                               bool
}
type artifact struct {
	Type, Name, Repository, Version, Digest, Reference string
	Sources                                            []artifactSource
}
type artifactSource struct{ Plane, Release, Requirement string }
type action struct {
	ID             string   `json:"id"`
	Owner          string   `json:"owner"`
	Needs          []string `json:"needs"`
	Phase          string   `json:"phase"`
	Preconditions  []string `json:"preconditions"`
	Postconditions []string `json:"postconditions"`
	Evidence       []string `json:"evidence"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	Recovery       string   `json:"recovery"`
}
type conditionalCheck struct {
	ID                string   `json:"id"`
	AppliesWhen       string   `json:"applies_when"`
	WhenApplicable    string   `json:"when_applicable"`
	WhenNotApplicable string   `json:"when_not_applicable"`
	WhenUnknown       string   `json:"when_unknown"`
	Evidence          []string `json:"evidence"`
}
type routeContract struct {
	SHA256                    string             `json:"-"`
	SchemaVersion             int                `json:"schema_version"`
	VersionScope              string             `json:"version_scope"`
	ID                        string             `json:"id"`
	From                      string             `json:"from"`
	To                        string             `json:"to"`
	Qualification             string             `json:"qualification"`
	SupportedExecutionEnabled *bool              `json:"supported_execution_enabled"`
	Actions                   []action           `json:"actions"`
	DisabledReleases          []string           `json:"disabled_releases"`
	ExecutionBlockers         []string           `json:"execution_blockers"`
	ConditionalChecks         []conditionalCheck `json:"conditional_checks,omitempty"`
	RuntimeArtifactChecks     []string           `json:"runtime_artifact_checks,omitempty"`
	RecoveryMode              string             `json:"recovery_mode,omitempty"`
}

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var routePattern = regexp.MustCompile(`^umbrella-[0-9]+\.[0-9]+\.[0-9]+-to-[0-9]+\.[0-9]+\.[0-9]+$`)
var planes = []string{"control-plane", "compute-plane", "observability"}

func checksum(raw []byte) string {
	h := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(h[:])
}
func loadLock(path string) (artifactLock, error) {
	var lock artifactLock
	raw, err := os.ReadFile(path)
	if err != nil {
		return lock, err
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		return lock, err
	}
	lock.SHA256 = checksum(raw)
	if lock.SchemaVersion != 2 || lock.VersionScope != "umbrella-release" || lock.Repository != "dsx-ai-factory/nvcf" {
		return lock, errors.New("unsupported artifact lock")
	}
	matrices := map[string]documentationMatrix{}
	for _, m := range lock.DocumentationMatrices {
		if m.Edition == "" || !commitPattern.MatchString(m.SourceCommit) || matrices[m.Edition].Edition != "" || len(m.Stacks) != len(planes) || m.URL != "https://docs.nvidia.com/nvcf/"+m.Edition+"/release-notes/compatibility-matrix" {
			return lock, errors.New("invalid or duplicate documentation matrix")
		}
		for _, p := range planes {
			if m.Stacks[p] == "" {
				return lock, errors.New("incomplete documentation matrix")
			}
		}
		matrices[m.Edition] = m
	}
	seen := map[string]bool{}
	inventories := map[string]stackRef{}
	for _, s := range lock.Selections {
		if s.Version == "" || seen[s.Version] || len(s.Stacks) != 3 {
			return lock, errors.New("invalid or duplicate selection")
		}
		seen[s.Version] = true
		matrix, ok := matrices[s.PeerMatrixEdition]
		if !ok {
			return lock, fmt.Errorf("peer documentation matrix for %s is not recorded", s.Version)
		}
		for _, p := range planes {
			v, ok := s.Stacks[p]
			prefix := map[string]string{"control-plane": "self-managed", "compute-plane": "nvcf-compute-plane", "observability": "observability"}[p]
			asset := map[string]string{"control-plane": "self-managed", "compute-plane": "compute-plane", "observability": "observability"}[p]
			if !ok || v.Version == "" || v.Tag != "deploy/stacks/"+prefix+"/v"+v.Version || !commitPattern.MatchString(v.Commit) || !digestPattern.MatchString(v.SHA256) || v.AssetID <= 0 || v.Asset != "nvcf-"+asset+"-stack-inventory.json" {
				return lock, fmt.Errorf("invalid %s identity for %s", p, s.Version)
			}
			if v.Version != matrix.Stacks[p] {
				return lock, fmt.Errorf("peer stack %s for %s differs from docs matrix %s", p, s.Version, matrix.Edition)
			}
			name := inventoryName(p, v)
			if previous, exists := inventories[name]; exists && previous != v {
				return lock, fmt.Errorf("conflicting inventory reference for %s", name)
			}
			inventories[name] = v
		}
	}
	for _, s := range lock.Selections {
		if err := validateRelease(s); err != nil {
			return lock, fmt.Errorf("endpoint %s: %w", s.Version, err)
		}
		if err := validateSelection(filepath.Dir(path), s); err != nil {
			return lock, fmt.Errorf("endpoint %s: %w", s.Version, err)
		}
	}
	return lock, nil
}
func (l artifactLock) selection(version string) (selection, error) {
	for _, s := range l.Selections {
		if s.Version == version {
			return s, nil
		}
	}
	return selection{}, fmt.Errorf("selection %q is not locked", version)
}
func loadRoute(path, id string, lock artifactLock) (routeContract, error) {
	var r routeContract
	if !routePattern.MatchString(id) {
		return r, errors.New("provide an umbrella route ID, for example umbrella-1.0.1-to-1.0.4")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	r.SHA256 = checksum(raw)
	if r.SchemaVersion != 2 || r.VersionScope != "umbrella-release" || r.ID != id || r.ID != "umbrella-"+r.From+"-to-"+r.To || r.From == r.To || r.Qualification != "unqualified" || r.SupportedExecutionEnabled == nil || *r.SupportedExecutionEnabled || len(r.Actions) == 0 {
		return r, errors.New("invalid or unsupported rehearsal contract")
	}
	if _, err := lock.selection(r.From); err != nil {
		return r, err
	}
	if _, err := lock.selection(r.To); err != nil {
		return r, err
	}
	seen := map[string]bool{}
	for _, a := range r.Actions {
		if a.ID == "" || seen[a.ID] || a.Owner == "" || a.TimeoutSeconds <= 0 || len(a.Preconditions) == 0 || len(a.Postconditions) == 0 || len(a.Evidence) == 0 || a.Recovery == "" || (a.Phase != "before-hop" && a.Phase != "during-hop" && a.Phase != "acceptance") {
			return r, fmt.Errorf("incomplete action %q", a.ID)
		}
		for _, n := range a.Needs {
			if !seen[n] {
				return r, fmt.Errorf("action %s needs unknown or later action %s", a.ID, n)
			}
		}
		seen[a.ID] = true
	}
	checks := map[string]bool{}
	for _, check := range r.RuntimeArtifactChecks {
		if strings.TrimSpace(check) == "" {
			return r, errors.New("runtime artifact checks must not be blank")
		}
	}
	for _, c := range r.ConditionalChecks {
		if c.ID == "" || checks[c.ID] || c.AppliesWhen == "" || c.WhenApplicable == "" || c.WhenNotApplicable == "" || c.WhenUnknown == "" || len(c.Evidence) == 0 {
			return r, fmt.Errorf("incomplete or duplicate conditional check %q", c.ID)
		}
		checks[c.ID] = true
	}
	return r, nil
}
func readJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}
func inventoryName(plane string, s stackRef) string { return plane + "-" + s.Version + ".json" }
func verifyInventory(raw []byte, s stackRef, plane string) (inventory, error) {
	var inv inventory
	if checksum(raw) != s.SHA256 {
		return inv, errors.New("inventory digest mismatch")
	}
	if err := json.Unmarshal(raw, &inv); err != nil {
		return inv, err
	}
	if inv.SchemaVersion != 1 || inv.Source.Tag != s.Tag || inv.Source.Version != s.Version || inv.Source.Commit != s.Commit || len(inv.Releases) == 0 || len(inv.Artifacts) == 0 {
		return inv, errors.New("inventory identity or contents mismatch")
	}
	for _, r := range inv.Releases {
		if r.Plane != plane {
			return inv, errors.New("inventory release belongs to another plane")
		}
	}
	return inv, nil
}
func loadInputs(lock artifactLock, r routeContract, dir string) (map[string]map[string]inventory, error) {
	loaded := map[string]map[string]inventory{}
	for _, version := range []string{r.From, r.To} {
		s, err := lock.selection(version)
		if err != nil {
			return nil, err
		}
		if err := verifyReleaseInputs(s, dir); err != nil {
			return nil, fmt.Errorf("endpoint %s: %w", version, err)
		}
		loaded[version] = map[string]inventory{}
		for _, p := range planes {
			ref := s.Stacks[p]
			raw, err := os.ReadFile(filepath.Join(dir, inventoryName(p, ref)))
			if err != nil {
				return nil, err
			}
			inv, err := verifyInventory(raw, ref, p)
			if err != nil {
				return nil, fmt.Errorf("%s %s: %w", version, p, err)
			}
			loaded[version][p] = inv
		}
	}
	return loaded, nil
}

type executor func(context.Context, string, ...string) ([]byte, error)

func execute(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
func fetchInputs(ctx context.Context, lock artifactLock, r routeContract, out string, timeout time.Duration) error {
	return fetchInputsWithExecutor(ctx, lock, r, out, timeout, execute)
}

func fetchInputsWithExecutor(ctx context.Context, lock artifactLock, r routeContract, out string, timeout time.Duration, exec executor) error {
	fetch := func(path string) ([]byte, error) {
		c, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		raw, err := exec(c, "gh", "api", "-H", "Accept: application/octet-stream", path)
		if err != nil {
			return nil, fmt.Errorf("GitHub input download failed for %s", path)
		}
		return raw, nil
	}
	for _, version := range []string{r.From, r.To} {
		selected, err := lock.selection(version)
		if err != nil {
			return err
		}
		if selected.Manifest == nil {
			continue
		}
		ref := selected.Manifest
		c, cancel := context.WithTimeout(ctx, timeout)
		raw, err := exec(c, "gh", "api", "-H", "Accept: application/vnd.github.raw+json", fmt.Sprintf("repos/%s/contents/deploy/releases/%s/manifest.json?ref=%s", lock.Repository, selected.Version, ref.SourceCommit))
		cancel()
		if err != nil {
			return fmt.Errorf("download umbrella manifest for %s: %w", selected.Version, err)
		}
		if checksum(raw) != ref.SHA256 {
			return fmt.Errorf("remote umbrella manifest digest mismatch for %s", selected.Version)
		}
	}
	for _, version := range []string{r.From, r.To} {
		s, _ := lock.selection(version)
		if s.Release == nil {
			continue
		}
		dir := releaseInputDir(out, s)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		for _, asset := range []releaseAsset{s.Release.Manifest, s.Release.AssetIndex, s.Release.Bundle} {
			raw, err := fetch(fmt.Sprintf("repos/%s/releases/assets/%d", lock.Repository, asset.AssetID))
			if err != nil {
				return err
			}
			if checksum(raw) != asset.SHA256 {
				return fmt.Errorf("release asset digest mismatch: %s", asset.Name)
			}
			if err := os.WriteFile(filepath.Join(dir, asset.Name), raw, 0600); err != nil {
				return err
			}
		}
		if err := verifyReleaseInputs(s, out); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, version := range []string{r.From, r.To} {
		s, _ := lock.selection(version)
		for _, p := range planes {
			ref := s.Stacks[p]
			name := inventoryName(p, ref)
			if seen[name] {
				continue
			}
			seen[name] = true
			raw, err := fetch(fmt.Sprintf("repos/%s/releases/assets/%d", lock.Repository, ref.AssetID))
			if err != nil {
				return err
			}
			if _, err := verifyInventory(raw, ref, p); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(out, name), raw, 0600); err != nil {
				return err
			}
		}
	}
	return writeJSON(filepath.Join(out, "fetch.json"), map[string]any{"route": r.ID, "verified_inventories": len(seen), "qualification": "unqualified"})
}
