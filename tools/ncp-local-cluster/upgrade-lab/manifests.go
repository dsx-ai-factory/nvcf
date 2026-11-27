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

type manifestRef struct {
	Path         string `json:"path"`
	SourceCommit string `json:"source_commit"`
	SHA256       string `json:"sha256"`
}

type manifestStack struct {
	stackRef
	Compatible map[string]string `json:"compatible_with"`
}
type umbrellaManifest struct {
	SchemaVersion int                      `json:"schema_version"`
	SourceCommit  string                   `json:"source_commit"`
	Version       string                   `json:"version"`
	Stacks        map[string]manifestStack `json:"stacks"`
}

func validateSelection(dir string, selected selection) error {
	if selected.Kind == "legacy-baseline" {
		if selected.Version != "1.0.1" || selected.PeerMatrixEdition != "1.0.1" || selected.Manifest != nil {
			return errors.New("legacy baseline must identify 1.0.1 without an umbrella manifest")
		}
		return nil
	}
	if selected.Kind != "umbrella-release" || selected.PeerMatrixEdition != selected.Version {
		return errors.New("unsupported selection kind")
	}
	ref := selected.Manifest
	if ref == nil || ref.Path != "manifests/"+selected.Version+".json" || !commitPattern.MatchString(ref.SourceCommit) || !digestPattern.MatchString(ref.SHA256) {
		return errors.New("invalid umbrella manifest reference")
	}
	raw, err := os.ReadFile(filepath.Join(dir, ref.Path))
	if err != nil {
		return fmt.Errorf("read umbrella manifest: %w", err)
	}
	if checksum(raw) != ref.SHA256 {
		return errors.New("umbrella manifest digest mismatch")
	}
	var manifest umbrellaManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return fmt.Errorf("decode umbrella manifest: %w", err)
	}
	if manifest.SchemaVersion != 2 || manifest.Version != selected.Version || len(manifest.Stacks) != len(planes) {
		return errors.New("umbrella manifest identity mismatch")
	}
	for _, plane := range planes {
		got, ok := manifest.Stacks[plane]
		want := selected.Stacks[plane]
		if !ok || got.Version != want.Version || got.Tag != want.Tag || got.Commit != want.Commit || got.Asset != want.Asset || got.SHA256 != want.SHA256 {
			return fmt.Errorf("%s selection differs from umbrella manifest", plane)
		}
		if len(got.Compatible) != len(planes)-1 {
			return errors.New("incomplete manifest peer matrix")
		}
		for _, peer := range planes {
			if peer != plane && got.Compatible[peer] != manifest.Stacks[peer].Version {
				return errors.New("inconsistent manifest peer matrix")
			}
		}
	}
	return nil
}
