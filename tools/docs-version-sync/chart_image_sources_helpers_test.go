// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"cmp"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const fixImplicitSource = "remove the source from the chart; the stack base.yaml or a values file supplies it"

// checkChartSources requires every image source field in the chart's own
// values.yaml to be written as an empty string: registry and repository of
// each indexed image, plus the source fields its image value exception adds.
// Omitting a field is not enough, because a subchart default then supplies
// the source (the vendored cert-manager chart defaults imageRegistry to
// quay.io); an empty string overrides it.
//
//	{registry: "", repository: "", tag: v1}       passes
//	{registry: docker.io, repository: "", ...}    fails: prefilled source
//	{registry: "", repository: docker.io/x, ...}  fails: registry host in repository
//	{repository: "", tag: v1}                     fails: registry not written
func (c *chartCheck) checkChartSources(values map[string]any, catalog []catalogImage) {
	chartFields := make(map[string]bool)
	for _, image := range catalog {
		node, err := imageCatalogValue(values, image.Path)
		if err != nil {
			c.fail(findingInvalidEntry, image.ID, image.Path, fmt.Sprintf("index path not in values.yaml: %v", err),
				"every indexed path to name an image object in the chart's own values.yaml",
				"write the image object, with empty registry and repository, at the indexed path")
			continue
		}
		for _, field := range append(append([]string{}, imageSourceFields...), image.Exception.imageSourceFields...) {
			c.checkSourceField(image.ID, image.Path+"."+field, node, field)
		}
		root, _, _ := strings.Cut(image.Path, ".")
		for _, field := range image.Exception.chartSourceFields {
			chartFields[root+"."+field] = true
		}
	}
	for _, path := range sortedKeys(chartFields) {
		root, field, _ := strings.Cut(path, ".")
		parent, _ := values[root].(map[string]any)
		c.checkSourceField("", path, parent, field)
	}
}

func (c *chartCheck) checkSourceField(id, path string, node map[string]any, field string) {
	value, written := node[field].(string)
	host, hasHost := imageReferenceHost(value)
	switch {
	case !written:
		c.fail(findingCoverage, id, path, "image source field not written; a subchart or template default can supply it",
			`""`, fmt.Sprintf(`write %s: "" in values.yaml`, path))
	case value == "":
	case field == "repository" && hasHost:
		c.fail(findingCoverage, id, path, fmt.Sprintf("repository %q contains registry host %s", value, host),
			`""`, fixImplicitSource)
	default:
		c.fail(findingCoverage, id, path, fmt.Sprintf("prefilled image source %q", value), `""`, fixImplicitSource)
	}
}

var (
	// templateAction matches one single-line template action.
	templateAction = regexp.MustCompile(`{{.*?}}`)
	// sourceDefault matches a non-empty string literal defaulted onto a field
	// that names an image source, such as .Values.image.registry or
	// .imageRegistry, with a quoted or raw string literal:
	//   .registry | default "x"          pipe form
	//   .registry | trim | default "x"   after argument-free functions
	//   default "x" (.registry)          prefix form, optionally parenthesized
	sourceDefault = regexp.MustCompile("(?i)" +
		`\.\w*(?:registry|repository)\s*(?:\|\s*\w+\s*)*\|\s*default\s+(?:"([^"]+)"|` + "`([^`]+)`" + `)` +
		`|\bdefault\s+(?:"([^"]+)"|` + "`([^`]+)`" + `)\s+\(?\s*[\w.$]*\.\w*(?:registry|repository)\b`)
	// imageField captures the value of an image: key or a --<name>-image=
	// argument.
	imageField = regexp.MustCompile(`(?:^\s*(?:-\s*)?"?image"?\s*:\s*|--[\w-]+-image=)("[^"]*"|'[^']*'|\S+)`)
)

// checkTemplateSources scans the chart's own templates/ for an implicit image
// source. Subcharts are not scanned: an upstream subchart is guarded by its
// wrapper, and a vendored one is listed as vendored-subchart.
//
//	image: docker.io/{{ .Values.image.repository }}    fails: literal registry host
//	{{ .Values.image.registry | default "nvcr.io" }}   fails: default source
//	{{ .Values.replicas | default 1 }}                 passes
//
// Known limit: the scan reads one line at a time and matches single-line
// actions, so a default split across lines, applied to a variable, placed
// after a function with arguments (| trimSuffix "/" |), or built with printf
// is not seen. The per-image guard renders still catch a default that fires.
// A literal host inside a multi-line comment is reported.
func (c *chartCheck) checkTemplateSources(chart string) {
	root := filepath.Join(chart, "templates")
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err == nil && entry.Type().IsRegular() && entry.Name() != imageCatalogProbeName {
			files = append(files, path)
		}
		return err
	})
	if err != nil && !os.IsNotExist(err) {
		c.renderError("templates", err)
		return
	}
	sort.Strings(files)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			c.renderError("templates", err)
			return
		}
		relative, _ := filepath.Rel(chart, file)
		for number, line := range strings.Split(string(raw), "\n") {
			location := fmt.Sprintf("%s:%d", relative, number+1)
			for _, problem := range implicitTemplateSources(line) {
				c.fail(findingCoverage, "", location, problem, "no implicit image source in chart templates", fixImplicitSource)
			}
		}
	}
}

// implicitTemplateSources returns each implicit image source on one template line.
func implicitTemplateSources(line string) []string {
	var problems []string
	for _, action := range templateAction.FindAllString(line, -1) {
		if strings.HasPrefix(strings.TrimLeft(action, "{- "), "/*") {
			continue
		}
		if match := sourceDefault.FindStringSubmatch(action); match != nil {
			// The first non-empty literal group: quoted or raw, pipe or prefix form.
			literal := cmp.Or(match[1:]...)
			problems = append(problems, fmt.Sprintf("default %q supplies an image source in %s", literal, action))
		}
	}
	for _, match := range imageField.FindAllStringSubmatch(line, -1) {
		value := strings.Trim(match[1], `"'`)
		// Actions become a placeholder segment, so only literal text can
		// start the reference: "nvcr.io/{{ .x }}" -> "nvcr.io/x" has a host,
		// "{{ .registry }}/nvcr.io" -> "x/nvcr.io" does not.
		if host, found := imageReferenceHost(templateAction.ReplaceAllString(value, "x")); found {
			problems = append(problems, fmt.Sprintf("image reference %s starts with literal registry host %s", value, host))
		}
	}
	return problems
}
