// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type componentChange struct {
	Plane    string `json:"plane"`
	Consumer string `json:"consumer"`
	Type     string `json:"type"`
	Name     string `json:"name"`
	From     string `json:"from"`
	To       string `json:"to"`
	Change   string `json:"change"`
	Review   string `json:"review,omitempty"`
}
type auditSummary struct {
	Route         string `json:"route"`
	Qualification string `json:"qualification"`
	Changed       int    `json:"changed"`
	Added         int    `json:"added"`
	Removed       int    `json:"removed"`
	Backward      int    `json:"backward"`
	Unordered     int    `json:"unordered"`
}
type auditReport struct {
	VersionScope          string             `json:"version_scope"`
	ComparisonScope       string             `json:"comparison_scope"`
	ArtifactLockSHA256    string             `json:"artifact_lock_sha256"`
	ContractSHA256        string             `json:"contract_sha256"`
	Summary               auditSummary       `json:"summary"`
	Selections            []selection        `json:"selections"`
	Components            []componentChange  `json:"components"`
	ExecutionBlockers     []string           `json:"execution_blockers"`
	ConditionalChecks     []conditionalCheck `json:"conditional_checks,omitempty"`
	RuntimeArtifactChecks []string           `json:"runtime_artifact_checks,omitempty"`
}
type component struct{ Plane, Consumer, Type, Name, Reference, Version string }

func inventoryComponents(invs map[string]inventory) (map[string]component, error) {
	components := map[string]component{}
	groups := map[string]map[string]component{}
	for _, p := range planes {
		inv := invs[p]
		for _, a := range inv.Artifacts {
			if a.Type == "" || a.Name == "" || a.Reference == "" || len(a.Sources) == 0 {
				return nil, fmt.Errorf("incomplete artifact in %s", p)
			}
			for _, s := range a.Sources {
				if s.Plane != p || s.Release == "" {
					return nil, fmt.Errorf("invalid artifact owner for %s", a.Name)
				}
				key := strings.Join([]string{p, s.Release, a.Type, a.Name}, "/")
				c := component{p, s.Release, a.Type, a.Name, a.Reference, a.Version}
				if groups[key] == nil {
					groups[key] = map[string]component{}
				}
				groups[key][c.Reference] = c
			}
		}
	}
	for key, variants := range groups {
		for ref, c := range variants {
			variantKey := key
			if len(variants) > 1 {
				variantKey += "/variant/" + ref
			}
			components[variantKey] = c
		}
	}
	return components, nil
}

func alignComponentVariants(from, to map[string]component) {
	endpoints := []map[string]component{from, to}
	variants := map[string]bool{}
	for _, components := range endpoints {
		for key, c := range components {
			base := strings.Join([]string{c.Plane, c.Consumer, c.Type, c.Name}, "/")
			if key != base {
				variants[base] = true
			}
		}
	}
	for _, components := range endpoints {
		for base := range variants {
			if c, exists := components[base]; exists {
				delete(components, base)
				components[base+"/variant/"+c.Reference] = c
			}
		}
	}
}

// Compare only matching version families. Different suffixes require review;
// guessing their ordering would conceal repository or packaging changes.
var plainVersion = regexp.MustCompile(`^v?([0-9]+)\.([0-9]+)\.([0-9]+)(.*)$`)

func compareVersion(a, b string) (int, bool) {
	if a == b {
		return 0, true
	}
	x, y := plainVersion.FindStringSubmatch(a), plainVersion.FindStringSubmatch(b)
	if x == nil || y == nil {
		return 0, false
	}
	suffixOrder := 0
	if x[4] != y[4] {
		if !strings.HasPrefix(x[4], "-nv-") || !strings.HasPrefix(y[4], "-nv-") {
			return 0, false
		}
		var known bool
		suffixOrder, known = compareVersion(strings.TrimPrefix(x[4], "-nv-"), strings.TrimPrefix(y[4], "-nv-"))
		if !known {
			return 0, false
		}
	}
	for i := 1; i <= 3; i++ {
		xv, xe := strconv.ParseUint(x[i], 10, 64)
		yv, ye := strconv.ParseUint(y[i], 10, 64)
		if xe != nil || ye != nil {
			return 0, false
		}
		if xv < yv {
			return -1, true
		}
		if xv > yv {
			return 1, true
		}
	}
	return suffixOrder, true
}
func compareRoute(lock artifactLock, r routeContract, in map[string]map[string]inventory) (auditReport, error) {
	report := auditReport{VersionScope: r.VersionScope, ComparisonScope: "release-inventories", ArtifactLockSHA256: lock.SHA256, ContractSHA256: r.SHA256, Summary: auditSummary{Route: r.ID, Qualification: "unqualified"}, ExecutionBlockers: r.ExecutionBlockers, ConditionalChecks: r.ConditionalChecks, RuntimeArtifactChecks: r.RuntimeArtifactChecks}
	for _, v := range []string{r.From, r.To} {
		s, err := lock.selection(v)
		if err != nil {
			return report, err
		}
		report.Selections = append(report.Selections, s)
	}
	from, err := inventoryComponents(in[r.From])
	if err != nil {
		return report, err
	}
	to, err := inventoryComponents(in[r.To])
	if err != nil {
		return report, err
	}
	alignComponentVariants(from, to)
	keys := map[string]bool{}
	for k := range from {
		keys[k] = true
	}
	for k := range to {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		a, aok := from[k]
		b, bok := to[k]
		c := b
		if !bok {
			c = a
		}
		row := componentChange{Plane: c.Plane, Consumer: c.Consumer, Type: c.Type, Name: c.Name, From: a.Reference, To: b.Reference, Change: "unchanged"}
		switch {
		case !aok:
			row.Change = "added"
			report.Summary.Added++
		case !bok:
			row.Change = "removed"
			row.Review = "Verify removal and old consumers before contract changes"
			report.Summary.Removed++
		case a.Reference != b.Reference:
			row.Change = "changed"
			report.Summary.Changed++
			cmp, ok := compareVersion(a.Version, b.Version)
			if !ok {
				row.Review = "Version order unknown; compare actual bytes and compatibility"
				report.Summary.Unordered++
			} else if cmp > 0 {
				row.Review = "Backward inventory selection; check enabled/rendered use before requiring compatibility evidence"
				report.Summary.Backward++
			} else if a.Version == b.Version {
				row.Review = "Same version at a different location; verify byte identity"
			}
		}
		if strings.Contains(k, "/variant/") {
			row.Review = "Multiple versions in this consumer; inspect rendered use without inferring a version pairing"
			report.Summary.Unordered++
		}
		report.Components = append(report.Components, row)
	}
	return report, nil
}
func renderComparison(r auditReport) string {
	var out strings.Builder
	fmt.Fprintf(&out, "# Component transition: %s\n\nQualification: unqualified. Inventory comparison does not prove runtime compatibility.\n\n", r.Summary.Route)
	out.WriteString("Route versions identify umbrella releases. Version 1.0.1 is a separate legacy baseline.\n\n")
	out.WriteString("Comparison includes optional releases. Inventory differences do not establish profile applicability or runtime failure.\n\n")
	out.WriteString("Blank cells mean no entry in that consumer's inventory. They do not establish runtime absence. Added and removed counts describe inventory entries.\n\n")
	for _, s := range r.Selections {
		if s.Release != nil {
			fmt.Fprintf(&out, "Endpoint %s original release: `%s` at `%s`. Manifest `%s`; bundle `%s`.\n\n", s.Version, s.Release.Tag, s.Release.Commit, s.Release.Manifest.SHA256, s.Release.Bundle.SHA256)
		}
		fmt.Fprintf(&out, "Endpoint %s reference: [Docs %s matrix](https://docs.nvidia.com/nvcf/%s/release-notes/compatibility-matrix).\n\n", s.Version, s.PeerMatrixEdition, s.PeerMatrixEdition)
	}
	fmt.Fprintf(&out, "Artifact lock: `%s`\n\nContract: `%s`\n\n", r.ArtifactLockSHA256, r.ContractSHA256)
	out.WriteString("| Endpoint | Plane | Stack version | Source tag | Source commit | Inventory SHA-256 |\n| --- | --- | --- | --- | --- | --- |\n")
	for _, s := range r.Selections {
		for _, p := range planes {
			v := s.Stacks[p]
			fmt.Fprintf(&out, "| %s | %s | %s | `%s` | `%s` | `%s` |\n", s.Version, p, v.Version, v.Tag, v.Commit, v.SHA256)
		}
	}
	fmt.Fprintf(&out, "\nChanged: %d; added: %d; removed: %d; backward: %d; unordered: %d.\n\n", r.Summary.Changed, r.Summary.Added, r.Summary.Removed, r.Summary.Backward, r.Summary.Unordered)
	if len(r.RuntimeArtifactChecks) > 0 {
		out.WriteString("## Runtime artifact checks\n\n")
		for _, check := range r.RuntimeArtifactChecks {
			fmt.Fprintf(&out, "- %s\n", check)
		}
		out.WriteString("\n")
	}
	if len(r.ExecutionBlockers) > 0 {
		out.WriteString("## Execution blockers\n\n")
		for _, blocker := range r.ExecutionBlockers {
			fmt.Fprintf(&out, "- %s\n", blocker)
		}
		out.WriteString("\n")
	}
	if len(r.ConditionalChecks) > 0 {
		out.WriteString("## Conditional checks\n\nApplicability requires effective source/target values and rendered manifests. The audit does not evaluate these rules.\n\n")
		for _, c := range r.ConditionalChecks {
			fmt.Fprintf(&out, "### %s\n\nApplies when: %s\n\n- When applicable: %s\n- When not applicable: %s\n- When unknown: %s\n\nRequired evidence:\n\n", c.ID, c.AppliesWhen, c.WhenApplicable, c.WhenNotApplicable, c.WhenUnknown)
			for _, e := range c.Evidence {
				fmt.Fprintf(&out, "- %s\n", e)
			}
			out.WriteString("\n")
		}
	}
	out.WriteString("## Component changes\n\n")
	out.WriteString("| Plane | Consumer | Type | Name | Source | Target | Review |\n| --- | --- | --- | --- | --- | --- | --- |\n")
	for _, c := range r.Components {
		if c.Change != "unchanged" {
			fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %s | %s |\n", c.Plane, c.Consumer, c.Type, c.Name, c.From, c.To, c.Review)
		}
	}
	return out.String()
}
