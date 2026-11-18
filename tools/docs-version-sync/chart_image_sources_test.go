// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestChartSourcesRequireEmptyWrittenFields covers each static source rule on
// a chart's own values.yaml, including the exception rows' extra fields.
func TestChartSourcesRequireEmptyWrittenFields(t *testing.T) {
	for _, test := range []struct {
		name, values, path, problem string
	}{
		{"empty registry and repository", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: \"\", tag: v1}\n", "", ""},
		{"prefilled registry", "ownedImages: {server: server.image}\nserver:\n  image: {registry: r.example, repository: \"\", tag: v1}\n", "server.image.registry", `prefilled image source "r.example"`},
		{"prefilled repository", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: team/server, tag: v1}\n", "server.image.repository", `prefilled image source "team/server"`},
		{"repository with a host", "ownedImages: {server: server.image}\nserver:\n  image: {registry: \"\", repository: docker.io/team/server}\n", "server.image.repository", "contains registry host docker.io"},
		{"registry not written", "ownedImages: {server: server.image}\nserver:\n  image: {repository: \"\", tag: v1}\n", "server.image.registry", "not written"},
		{"null registry", "ownedImages: {server: server.image}\nserver:\n  image: {registry: null, repository: \"\"}\n", "server.image.registry", "not written"},
		{"nats fullImageName prefilled", "thirdPartyImages: {box: nats.box.image}\nnats:\n  box:\n    image: {registry: \"\", repository: \"\", fullImageName: r.example/box:1}\n", "nats.box.image.fullImageName", "prefilled"},
		{"cert-manager shared registry prefilled", "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: quay.io\n  imageNamespace: \"\"\n  image: {registry: \"\", repository: \"\"}\n", "cert-manager.imageRegistry", `prefilled image source "quay.io"`},
		{"cert-manager shared namespace not written", "thirdPartyImages: {controller: cert-manager.image}\ncert-manager:\n  imageRegistry: \"\"\n  image: {registry: \"\", repository: \"\"}\n", "cert-manager.imageNamespace", "not written"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var values map[string]any
			if err := yaml.Unmarshal([]byte(test.values), &values); err != nil {
				t.Fatal(err)
			}
			catalog, err := readImageIndex(values, map[string]string{"nats": "nats", "cert-manager": "cert-manager"})
			if err != nil {
				t.Fatal(err)
			}
			check := &chartCheck{chart: "c"}
			check.checkChartSources(values, catalog)
			if test.problem == "" {
				if len(check.findings) != 0 {
					t.Fatalf("unexpected findings %+v", check.findings)
				}
				return
			}
			if len(check.findings) != 1 || check.findings[0].ValuesPath != test.path ||
				check.findings[0].Category != findingCoverage || !strings.Contains(check.findings[0].Problem, test.problem) {
				t.Fatalf("findings %+v, want one at %s containing %q", check.findings, test.path, test.problem)
			}
		})
	}
}

// TestChartSourcesRejectMissingIndexPath makes an index that points nowhere a
// configuration error, not an expected pending-fix finding.
func TestChartSourcesRejectMissingIndexPath(t *testing.T) {
	values := map[string]any{"ownedImages": map[string]any{"server": "server.image"}}
	catalog, err := readImageIndex(values, nil)
	if err != nil {
		t.Fatal(err)
	}
	check := &chartCheck{chart: "c"}
	check.checkChartSources(values, catalog)
	findings, expected := pendingFixOutcome(check, chartImageExclusion{Reason: reasonPendingFix})
	if expected != "" || len(findings) != 1 || findings[0].Category != findingInvalidEntry {
		t.Fatalf("findings %+v, expected label %q", findings, expected)
	}
}

// TestImplicitTemplateSources pins what the template scan reports.
func TestImplicitTemplateSources(t *testing.T) {
	for _, test := range []struct {
		line, problem string
	}{
		{`image: docker.io/{{ .Values.image.repository }}:{{ .Values.image.tag }}`, "literal registry host docker.io"},
		{`  - image: "nvcr.io/nvidia/x:1"`, "literal registry host nvcr.io"},
		{`        - --acme-http01-solver-image=quay.io/jetstack/solver:v1`, "literal registry host quay.io"},
		{`image: {{ .Values.image.registry | default "nvcr.io" }}/{{ .Values.image.repository }}`, `default "nvcr.io"`},
		{`{{- $registry := .Values.global.imageRegistry | default "quay.io" -}}`, `default "quay.io"`},
		{`{{ default "docker.io" .Values.image.registry }}`, `default "docker.io"`},
		{`image: {{ .Values.image.registry }}/{{ .Values.image.repository }}:{{ .Values.image.tag }}`, ""},
		{`image: "{{ include "openbao.image" (dict "image" .Values.image "name" "x") }}"`, ""},
		{`replicas: {{ .Values.replicas | default 1 }}`, ""},
		{`tag: {{ .Values.image.tag | default .Chart.AppVersion }}`, ""},
		{`{{ .Values.image.registry | default "" }}`, ""},
		{`{{/* .Values.image.registry | default "docker.io" */}}`, ""},
		{"{{ .Values.image.registry | default `nvcr.io` }}", "default \"nvcr.io\""},
		{"{{ default `nvcr.io` .Values.image.registry }}", "default \"nvcr.io\""},
		{`{{ .Values.image.registry | trim | default "nvcr.io" }}`, `default "nvcr.io"`},
		{`{{ default "docker.io" (.Values.global.imageRegistry) }}`, `default "docker.io"`},
		{`imagePullPolicy: {{ .Values.image.pullPolicy | default "IfNotPresent" }}`, ""},
		{`{{ .Values.registryPort | default "5000" }}`, ""},
		// The default applies to the tag, not to a source field.
		{`image: {{ printf "%s/%s:%s" .Values.image.registry .Values.image.repository (.Values.image.tag | default "latest") }}`, ""},
		{`image: {{ .Values.registry }}/nvcr.io`, ""},
	} {
		problems := implicitTemplateSources(test.line)
		if test.problem == "" {
			if len(problems) != 0 {
				t.Errorf("%s: unexpected %v", test.line, problems)
			}
			continue
		}
		if len(problems) != 1 || !strings.Contains(problems[0], test.problem) {
			t.Errorf("%s: problems %v, want one containing %q", test.line, problems, test.problem)
		}
	}
}

// TestGuardErrorNamesPath requires the guard error to name the exact values
// path, not a longer path that starts with it.
func TestGuardErrorNamesPath(t *testing.T) {
	for _, test := range []struct {
		message string
		names   bool
	}{
		{"execution error: A valid image registry is required for x.image.registry", true},
		{"execution error: set x.image.registry.", true},
		{"execution error: set x.image.registryMirror", false},
		{"execution error: set x.image.registry.sub", false},
		{`execution error: "set x.image.registry."`, true},
		{"execution error: set y.x.image.registry", false},
		{"execution error: image source missing", false},
	} {
		if got := guardNamesPath(test.message, "x.image.registry"); got != test.names {
			t.Errorf("%q: names path %v, want %v", test.message, got, test.names)
		}
	}
}
