// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type releaseAsset struct {
	Name    string `json:"name"`
	SHA256  string `json:"sha256"`
	AssetID int64  `json:"asset_id"`
}

type releaseRef struct {
	Tag        string       `json:"source_tag"`
	Commit     string       `json:"source_commit"`
	Manifest   releaseAsset `json:"manifest"`
	AssetIndex releaseAsset `json:"asset_index"`
	Bundle     releaseAsset `json:"bundle"`
}

func validateRelease(s selection) error {
	if s.Kind == "legacy-baseline" {
		if s.Release != nil {
			return errors.New("legacy baseline must not claim an umbrella release bundle")
		}
		return nil
	}
	ref := s.Release
	if ref == nil || s.Manifest == nil || ref.Tag != "nvcf/"+s.Version || ref.Commit != s.Manifest.SourceCommit || ref.Manifest.SHA256 != s.Manifest.SHA256 {
		return errors.New("umbrella release assets must match the pinned manifest identity")
	}
	for _, entry := range []struct {
		asset releaseAsset
		name  string
	}{{ref.Manifest, "manifest.json"}, {ref.AssetIndex, "release-assets.json"}, {ref.Bundle, "nvcf-" + s.Version + ".tar.gz"}} {
		if entry.asset.Name != entry.name || !digestPattern.MatchString(entry.asset.SHA256) || entry.asset.AssetID <= 0 {
			return fmt.Errorf("invalid product release asset: %s", entry.name)
		}
	}
	return nil
}

func releaseInputDir(root string, s selection) string {
	return filepath.Join(root, "releases", s.Version)
}

func verifyReleaseInputs(s selection, root string) error {
	if s.Release == nil {
		return nil
	}
	read := func(asset releaseAsset) ([]byte, error) {
		raw, err := os.ReadFile(filepath.Join(releaseInputDir(root, s), asset.Name))
		if err != nil {
			return nil, fmt.Errorf("read product %s: %w", asset.Name, err)
		}
		if checksum(raw) != asset.SHA256 {
			return nil, fmt.Errorf("release asset digest mismatch: %s", asset.Name)
		}
		return raw, nil
	}
	raw, err := read(s.Release.Manifest)
	if err != nil {
		return err
	}
	var manifest umbrellaManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("decode product manifest: %w", err)
	}
	if manifest.SchemaVersion != 2 || manifest.Version != s.Version || !commitPattern.MatchString(manifest.SourceCommit) || len(manifest.Stacks) != len(planes) {
		return errors.New("product manifest identity mismatch")
	}
	for _, plane := range planes {
		p, ok := manifest.Stacks[plane]
		ref := s.Stacks[plane]
		if !ok || p.Version != ref.Version || p.Tag != ref.Tag || p.Commit != ref.Commit || p.Asset != ref.Asset || p.SHA256 != ref.SHA256 {
			return fmt.Errorf("product manifest stack selection mismatch: %s", plane)
		}
	}
	raw, err = read(s.Release.AssetIndex)
	if err != nil {
		return err
	}
	var index struct {
		SchemaVersion int               `json:"schema_version"`
		Version       string            `json:"version"`
		Assets        map[string]string `json:"assets"`
	}
	if err := json.Unmarshal(raw, &index); err != nil {
		return fmt.Errorf("decode product asset index: %w", err)
	}
	if index.SchemaVersion != 1 || index.Version != s.Version || index.Assets[s.Release.Manifest.Name] != s.Release.Manifest.SHA256 || index.Assets[s.Release.Bundle.Name] != s.Release.Bundle.SHA256 {
		return errors.New("product asset index identity mismatch")
	}
	for _, ref := range s.Stacks {
		if index.Assets[ref.Asset] != ref.SHA256 {
			return errors.New("product asset index inventory mismatch")
		}
	}
	_, err = read(s.Release.Bundle)
	return err
}
