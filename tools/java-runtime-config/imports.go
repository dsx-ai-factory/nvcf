// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

type workspaceList []string

func (list *workspaceList) String() string { return strings.Join(*list, ",") }
func (list *workspaceList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("workspace directory must not be empty")
	}
	*list = append(*list, value)
	return nil
}

type rcGuard struct {
	home, workdir, workspace string
	visiting                 map[string]bool
}

type sourceLine struct {
	text   string
	number int
}

// Keep physical source locations while joining Bazel's LF/CRLF continuations.
func sourceLines(input string) []sourceLine {
	var lines []sourceLine
	var text strings.Builder
	start, number := 1, 1
	for _, physical := range strings.SplitAfter(input, "\n") {
		if physical == "" {
			continue
		}
		terminated := strings.HasSuffix(physical, "\n")
		body := strings.TrimSuffix(physical, "\n")
		if terminated {
			body = strings.TrimSuffix(body, "\r")
		}
		if terminated && strings.HasSuffix(body, "\\") {
			text.WriteString(strings.TrimSuffix(body, "\\"))
			number++
			continue
		}
		text.WriteString(body)
		lines = append(lines, sourceLine{text.String(), start})
		text.Reset()
		number++
		start = number
	}
	if text.Len() != 0 {
		lines = append(lines, sourceLine{text.String(), start})
	}
	return lines
}

var unresolvedPlaceholder = regexp.MustCompile(`%[^%/]+%`)

func (guard *rcGuard) resolve(path string) (string, error) {
	path = strings.ReplaceAll(path, "%workspace%", guard.workspace)
	path = strings.ReplaceAll(path, "%user.home%", guard.home)
	if unresolvedPlaceholder.MatchString(path) {
		return "", fmt.Errorf("unsupported rc path placeholder")
	}
	// Bazel resolves relative imports against its working directory, not the
	// directory containing the importing file.
	if !filepath.IsAbs(path) {
		path = filepath.Join(guard.workdir, path)
	}
	return filepath.Clean(path), nil
}

func (guard *rcGuard) file(path string, optional bool) error {
	resolved, err := guard.resolve(path)
	if err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		if optional && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("%s: cannot resolve rc file", resolved)
	}
	if guard.visiting[canonical] {
		return fmt.Errorf("%s: rc import cycle", resolved)
	}
	input, err := os.ReadFile(canonical)
	if err != nil {
		return fmt.Errorf("%s: cannot read rc file", resolved)
	}
	guard.visiting[canonical] = true
	defer delete(guard.visiting, canonical)
	return guard.contents(string(input), resolved, false)
}

func (guard *rcGuard) contents(input, path string, allowFixed bool) error {
	for _, line := range sourceLines(input) {
		words, err := tokenize(line.text)
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, line.number, err)
		}
		if len(words) == 0 {
			continue
		}
		switch words[0].value {
		case "import", "try-import":
			if len(words) != 2 {
				return fmt.Errorf("%s:%d: invalid rc import", path, line.number)
			}
			if err := guard.file(words[1].value, words[0].value == "try-import"); err != nil {
				return fmt.Errorf("%s:%d: %w", path, line.number, err)
			}
			continue
		case "try-import-if-bazel-version":
			return fmt.Errorf("%s:%d: conditional rc imports cannot be verified by CI Java setup", path, line.number)
		}
		if allowFixed {
			continue
		}
		for i := 1; i < len(words); i++ {
			option, assignment, joined := strings.Cut(words[i].value, "=")
			if option != "--action_env" && option != "--host_action_env" && option != "--test_env" {
				continue
			}
			if !joined && i+1 < len(words) {
				i++
				assignment = words[i].value
			}
			name, _, fixed := strings.Cut(assignment, "=")
			if name == "JAVA_TOOL_OPTIONS" && fixed {
				return fmt.Errorf("%s:%d: fixed JAVA_TOOL_OPTIONS in %s; move options to the CI shell or inline home rc before retrying", path, line.number, option)
			}
		}
	}
	return nil
}

func workspaceRoot(dir string) (string, error) {
	for candidate := dir; ; candidate = filepath.Dir(candidate) {
		for _, marker := range []string{"MODULE.bazel", "WORKSPACE.bazel", "WORKSPACE"} {
			if info, err := os.Stat(filepath.Join(candidate, marker)); err == nil && !info.IsDir() {
				return candidate, nil
			}
		}
		if filepath.Dir(candidate) == candidate {
			break
		}
	}
	return "", fmt.Errorf("%s: no Bazel workspace found", dir)
}

func checkRCs(input, cwd, home string, workspaces []string, extraRC, systemRC string) error {
	if len(workspaces) == 0 {
		workspaces = []string{cwd}
	}
	for _, workdir := range workspaces {
		if !filepath.IsAbs(workdir) {
			workdir = filepath.Join(cwd, workdir)
		}
		info, err := os.Stat(workdir)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("%s: workspace directory unavailable", workdir)
		}
		root, err := workspaceRoot(filepath.Clean(workdir))
		if err != nil {
			return err
		}
		guard := rcGuard{home: home, workdir: workdir, workspace: root, visiting: map[string]bool{}}
		if systemRC != "" {
			if err := guard.file(systemRC, true); err != nil {
				return err
			}
		}
		if err := guard.file(filepath.Join(root, ".bazelrc"), true); err != nil {
			return err
		}
		homeRC := filepath.Join(home, ".bazelrc")
		// Inline home values can be merged; imported files are never rewritten.
		if err := guard.contents(input, homeRC, true); err != nil {
			return err
		}
		if extraRC != "" {
			for _, path := range strings.Split(extraRC, ",") {
				if path == "" {
					return fmt.Errorf("BAZELRC contains an empty path")
				}
				if err := guard.file(path, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func runCLI(in io.Reader, out io.Writer, cwd string, workspaces []string) error {
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	systemRC := "/etc/bazel.bazelrc"
	if runtime.GOOS == "windows" {
		systemRC = filepath.Join(os.Getenv("ProgramData"), "bazel.bazelrc")
	}
	return configure(in, out, func(input string) error {
		return checkRCs(input, cwd, home, workspaces, os.Getenv("BAZELRC"), systemRC)
	})
}
