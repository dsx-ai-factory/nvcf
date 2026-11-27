// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type probeResult struct {
	Reference string `json:"reference"`
	Required  bool   `json:"required"`
	Status    string `json:"status"`
	SHA256    string `json:"sha256,omitempty"`
	Error     string `json:"error,omitempty"`
	RawOutput string `json:"raw_output"`
}
type accessReport struct {
	ContractSHA256 string        `json:"contract_sha256"`
	Route          string        `json:"route"`
	Qualification  string        `json:"qualification"`
	Status         string        `json:"status"`
	Complete       bool          `json:"complete"`
	StartedAt      time.Time     `json:"started_at"`
	FinishedAt     time.Time     `json:"finished_at"`
	Results        []probeResult `json:"results"`
}

func platformDigest(raw []byte) string {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	var visit func(any) string
	visit = func(v any) string {
		switch x := v.(type) {
		case []any:
			for _, i := range x {
				if d := visit(i); d != "" {
					return d
				}
			}
		case map[string]any:
			if p, ok := x["platform"].(map[string]any); ok && p["os"] == "linux" && p["architecture"] == "amd64" {
				if d, ok := x["digest"].(string); ok && digestPattern.MatchString(d) {
					return d
				}
			}
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if d := visit(x[k]); d != "" {
					return d
				}
			}
		}
		return ""
	}
	return visit(value)
}
func probeRoute(ctx context.Context, r routeContract, in map[string]map[string]inventory, out string, timeout time.Duration, exec executor) error {
	report := accessReport{ContractSHA256: r.SHA256, Route: r.ID, Qualification: "unqualified", Status: "running", StartedAt: time.Now().UTC()}
	blocked := false
	private := filepath.Join(out, "private")
	if err := os.Mkdir(private, 0700); err != nil {
		return err
	}
	repositories := map[string]string{}
	if config := os.Getenv("UPGRADE_LAB_HELM_REPOSITORY_CONFIG"); config != "" {
		var file struct{ Repositories []struct{ Name, URL string } }
		if err := readJSON(config, &file); err != nil {
			return errors.New("read private Helm repositories JSON failed")
		}
		for _, repo := range file.Repositories {
			repositories[repo.URL] = repo.Name
		}
	}
	type item struct {
		a        artifact
		required bool
	}
	items := map[string]item{}
	for _, v := range []string{r.From, r.To} {
		for _, p := range planes {
			requiredReleases := map[string]bool{}
			for _, rel := range in[v][p].Releases {
				requiredReleases[rel.Name] = rel.Required
			}
			for _, a := range in[v][p].Artifacts {
				if a.Type != "container-image" && a.Type != "helm-chart" {
					continue
				}
				old := items[a.Type+"/"+a.Reference]
				old.a = a
				for _, s := range a.Sources {
					if s.Requirement != "" && s.Requirement != "required" && s.Requirement != "optional" {
						return errors.New("unknown artifact requirement")
					}
					disabled := false
					for _, name := range r.DisabledReleases {
						if s.Plane+"/"+s.Release == name {
							disabled = true
						}
					}
					if !disabled && (s.Requirement == "required" || (s.Requirement == "" && requiredReleases[s.Release])) {
						old.required = true
					}
				}
				items[a.Type+"/"+a.Reference] = old
			}
		}
	}
	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if ctx.Err() != nil {
			blocked = true
			break
		}
		it := items[k]
		a := it.a
		result := probeResult{Reference: a.Reference, Required: it.required, Status: "skipped", RawOutput: fmt.Sprintf("private/%04d.log", i)}
		if !it.required {
			report.Results = append(report.Results, result)
			continue
		}
		name, args := "docker", []string{"manifest", "inspect", "--verbose", a.Reference}
		sourceChart := false
		if a.Type == "helm-chart" {
			u, err := url.Parse(a.Repository)
			if err != nil || u.Scheme != "https" {
				return fmt.Errorf("unsupported chart repository for %s", a.Name)
			}
			name, args = "helm", []string{"pull", a.Name, "--repo", a.Repository, "--version", a.Version, "--destination", private}
			if alias := repositories[a.Repository]; alias != "" {
				name, args = "env", []string{"HELM_REPOSITORY_CONFIG=" + os.Getenv("UPGRADE_LAB_HELM_REPOSITORY_CONFIG"), "helm", "pull", alias + "/" + a.Name, "--version", a.Version, "--destination", private}
			}
			if strings.HasPrefix(a.Repository, "https://github.com/") {
				commit, path, err := sourceChartLocation(a.Repository, a.Name, in)
				if err != nil {
					return err
				}
				sourceChart = true
				name, args = "git", []string{"-C", os.Getenv("UPGRADE_LAB_SOURCE_REPO"), "archive", "--format=tar", commit + ":" + path + "/" + a.Name}
			}
		}
		c, cancel := context.WithTimeout(ctx, timeout)
		raw, err := exec(c, name, args...)
		cancel()
		if e := os.WriteFile(filepath.Join(out, result.RawOutput), raw, 0600); e != nil {
			return e
		}
		result.Status = "passed"
		if err != nil {
			result.Status = "blocked"
			result.Error = "artifact access failed; inspect private output"
		} else if a.Type == "container-image" {
			result.SHA256 = platformDigest(raw)
			if result.SHA256 == "" {
				result.Status = "blocked"
				result.Error = "linux/amd64 manifest digest is missing"
			}
		} else if sourceChart {
			result.SHA256 = checksum(raw)
		} else {
			archive := filepath.Join(private, a.Name+"-"+a.Version+".tgz")
			data, e := os.ReadFile(archive)
			if e != nil {
				result.Status = "blocked"
				result.Error = "downloaded chart archive is missing"
			} else {
				result.SHA256 = checksum(data)
			}
		}
		if result.Status != "passed" {
			blocked = true
		}
		report.Results = append(report.Results, result)
		report.FinishedAt = time.Now().UTC()
		if err := writeJSON(filepath.Join(out, "access.json"), report); err != nil {
			return err
		}
		fmt.Printf("%s %s\n", result.Status, a.Reference)
	}
	report.FinishedAt = time.Now().UTC()
	report.Complete = ctx.Err() == nil && len(report.Results) == len(items)
	report.Status = "passed"
	if blocked || !report.Complete || len(items) == 0 {
		report.Status = "blocked"
	}
	if err := writeJSON(filepath.Join(out, "access.json"), report); err != nil {
		return err
	}
	if report.Status != "passed" {
		return errors.New("artifact access blocked; workloads were not changed")
	}
	return nil
}

func sourceChartLocation(repository, name string, in map[string]map[string]inventory) (string, string, error) {
	validName := regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	if !validName.MatchString(name) {
		return "", "", errors.New("invalid source chart name")
	}
	for _, prefix := range []string{"https://github.com/NVIDIA/nvcf/tree/", "https://github.com/dsx-ai-factory/nvcf/tree/"} {
		if !strings.HasPrefix(repository, prefix) {
			continue
		}
		commit, path, ok := strings.Cut(strings.TrimPrefix(repository, prefix), "/")
		if !ok || !commitPattern.MatchString(commit) {
			break
		}
		for _, set := range in {
			for plane, inv := range set {
				stack := map[string]string{"control-plane": "self-managed", "compute-plane": "nvcf-compute-plane", "observability": "observability"}[plane]
				if inv.Source.Commit == commit && path == "deploy/stacks/"+stack+"/charts" {
					return commit, path, nil
				}
			}
		}
	}
	return "", "", errors.New("source chart does not belong to a locked stack commit")
}
