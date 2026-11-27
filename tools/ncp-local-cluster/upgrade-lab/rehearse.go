// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type rehearsalOverlay struct {
	Values  string `json:"values"`
	Secrets string `json:"secrets"`
}

type rehearsalConfig struct {
	SourceRepository     string                                 `json:"source_repository"`
	Inputs               string                                 `json:"inputs"`
	Helmfile             string                                 `json:"helmfile"`
	BundleVerifier       string                                 `json:"bundle_verifier"`
	Overlays             map[string]map[string]rehearsalOverlay `json:"overlays"`
	ComputeRegistrations []rehearsalRegistration                `json:"compute_registrations"`
}

type rehearsalRegistration struct {
	ClusterName string `json:"cluster_name"`
	NCAID       string `json:"nca_id"`
	Values      string `json:"values"`
}

type rehearsalSource struct {
	Endpoint      string   `json:"endpoint"`
	Plane         string   `json:"plane"`
	Ref           stackRef `json:"ref"`
	SourceKind    string   `json:"source_kind"`
	ArchiveSHA256 string   `json:"archive_sha256"`
	ValuesSHA256  string   `json:"values_sha256"`
	SecretsSHA256 string   `json:"secrets_sha256"`
}

type rehearsalStep struct {
	Operation string `json:"operation"`
	Status    string `json:"status"`
	Output    string `json:"output"`
	SHA256    string `json:"sha256"`
}

type rehearsalRecord struct {
	SchemaVersion        int                   `json:"schema_version"`
	Route                string                `json:"route"`
	Phase                string                `json:"phase"`
	Status               string                `json:"status"`
	Qualification        string                `json:"qualification"`
	ContractSHA256       string                `json:"contract_sha256"`
	LockSHA256           string                `json:"lock_sha256"`
	ConfigSHA256         string                `json:"config_sha256"`
	BinarySHA256         string                `json:"binary_sha256"`
	HelmfileSHA256       string                `json:"helmfile_sha256"`
	BundleVerifierSHA256 string                `json:"bundle_verifier_sha256"`
	ReleaseInputs        map[string]releaseRef `json:"release_inputs"`
	StartedAt            time.Time             `json:"started_at"`
	FinishedAt           *time.Time            `json:"finished_at,omitempty"`
	Sources              []rehearsalSource     `json:"sources"`
	Steps                []rehearsalStep       `json:"steps"`
	Renders              map[string]string     `json:"render_sha256"`
	Registrations        map[string]string     `json:"registration_sha256"`
	NextSafeAction       string                `json:"next_safe_action"`
}

var stackDirectories = map[string]string{
	"control-plane": "self-managed", "compute-plane": "nvcf-compute-plane", "observability": "observability",
}

var clusterNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

var errComputeRegistrationsMissing = errors.New("control-plane and observability renders completed; compute renders require both retained registration files from the source baseline")

func runRehearsal(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("rehearse", flag.ContinueOnError)
	contracts := f.String("contracts", "tools/ncp-local-cluster/upgrade-lab/transitions", "transition contract directory")
	routeID := f.String("route", "", "exact route identity")
	runDir := f.String("run-dir", "", "new private rehearsal directory")
	config := f.String("run-config", "", "private JSON configuration")
	phase := f.String("phase", "prepare", "implemented phase: prepare")
	timeout := f.Duration("timeout", 10*time.Minute, "timeout per external command")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *runDir == "" || *config == "" || *timeout <= 0 {
		return errors.New("rehearse requires --route, --run-dir, --run-config and a positive timeout")
	}
	if *phase != "prepare" {
		return errors.New("only prepare is implemented; baseline, backup, apply, restore and verify require the manual rehearsal procedure")
	}
	lock, err := loadLock(filepath.Join(*contracts, "artifact-lock.json"))
	if err != nil {
		return err
	}
	route, err := loadRoute(filepath.Join(*contracts, *routeID+".json"), *routeID, lock)
	if err != nil {
		return err
	}
	return prepareRehearsal(ctx, lock, route, *runDir, *config, *timeout, execute)
}

func readRehearsalConfig(path string) (rehearsalConfig, []byte, error) {
	var config rehearsalConfig
	raw, err := os.ReadFile(path)
	if err != nil {
		return config, nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return config, nil, errors.New("invalid private rehearsal configuration")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return config, nil, errors.New("private rehearsal configuration must contain one JSON object")
	}
	for _, p := range []string{config.SourceRepository, config.Inputs, config.Helmfile, config.BundleVerifier} {
		if !filepath.IsAbs(p) {
			return config, nil, errors.New("source_repository, inputs, helmfile and bundle_verifier must be absolute paths")
		}
	}
	if len(config.Overlays) != 2 {
		return config, nil, errors.New("provide source and target overlays")
	}
	if len(config.ComputeRegistrations) != 0 && len(config.ComputeRegistrations) != 2 {
		return config, nil, errors.New("provide both compute registrations or omit them before baseline registration")
	}
	seen := map[string]bool{}
	for _, registration := range config.ComputeRegistrations {
		if seen[registration.ClusterName] || len(registration.ClusterName) > 63 || !clusterNamePattern.MatchString(registration.ClusterName) || registration.NCAID == "" || strings.ContainsAny(registration.NCAID, "\r\n\x00") || !filepath.IsAbs(registration.Values) {
			return config, nil, errors.New("compute registrations require two distinct DNS cluster names, nca_id and absolute values paths")
		}
		seen[registration.ClusterName] = true
	}
	for _, endpoint := range []string{"source", "target"} {
		if len(config.Overlays[endpoint]) != len(planes) {
			return config, nil, errors.New("each endpoint requires overlays for all three planes")
		}
		for _, plane := range planes {
			o := config.Overlays[endpoint][plane]
			if !filepath.IsAbs(o.Values) || !filepath.IsAbs(o.Secrets) {
				return config, nil, errors.New("overlay values and secrets must be absolute paths")
			}
		}
	}
	return config, raw, nil
}

func fileChecksum(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return checksum(raw), nil
}

func renderedTreeChecksum(root string) (string, error) {
	var entries []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("render output must not contain symbolic links")
		}
		digest, err := fileChecksum(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, filepath.ToSlash(relative)+" "+digest)
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", errors.New("render output contains no files")
	}
	return checksum([]byte(strings.Join(entries, "\n"))), nil
}

func prepareRehearsal(ctx context.Context, lock artifactLock, route routeContract, out, configPath string, timeout time.Duration, exec executor) (result error) {
	config, raw, err := readRehearsalConfig(configPath)
	if err != nil {
		return err
	}
	if _, err := loadInputs(lock, route, config.Inputs); err != nil {
		return err
	}
	out, err = filepath.Abs(out)
	if err != nil {
		return err
	}
	if err := os.Mkdir(out, 0700); err != nil {
		return fmt.Errorf("rehearsal directory must be new: %w", err)
	}
	record := rehearsalRecord{
		SchemaVersion: 2, Route: route.ID, Phase: "prepare", Status: "running", Qualification: "unqualified",
		ContractSHA256: route.SHA256, LockSHA256: lock.SHA256, ConfigSHA256: checksum(raw), StartedAt: time.Now().UTC(),
		NextSafeAction: "Wait for preparation to complete; do not apply releases.",
		Renders:        map[string]string{},
		Registrations:  map[string]string{},
		ReleaseInputs:  map[string]releaseRef{},
	}
	save := func() error {
		temporary := filepath.Join(out, "record.tmp")
		if err := writeJSON(temporary, record); err != nil {
			return err
		}
		return os.Rename(temporary, filepath.Join(out, "record.json"))
	}
	defer func() {
		now := time.Now().UTC()
		record.FinishedAt = &now
		if result != nil {
			record.Status = "blocked"
			record.NextSafeAction = "Inspect private command output, resolve the preparation failure, and start a new run. Do not apply releases."
			if errors.Is(result, errComputeRegistrationsMissing) {
				record.NextSafeAction = "Review completed control-plane and observability renders before creating the isolated source baseline. Retain both source registrations, then prepare again. Do not upgrade."
			}
		}
		result = errors.Join(result, save())
	}()
	if err := os.Mkdir(filepath.Join(out, "private"), 0700); err != nil {
		return err
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	if record.BinarySHA256, err = fileChecksum(binary); err != nil {
		return err
	}
	if record.HelmfileSHA256, err = fileChecksum(config.Helmfile); err != nil {
		return err
	}
	if record.BundleVerifierSHA256, err = fileChecksum(config.BundleVerifier); err != nil {
		return err
	}
	command := func(operation, name string, args ...string) ([]byte, error) {
		log := fmt.Sprintf("private/%04d.log", len(record.Steps))
		step := rehearsalStep{Operation: operation, Status: "running", Output: log}
		record.Steps = append(record.Steps, step)
		if err := save(); err != nil {
			return nil, err
		}
		limited, cancel := context.WithTimeout(ctx, timeout)
		data, err := exec(limited, name, args...)
		cancel()
		step.Status, step.SHA256 = "passed", checksum(data)
		if err != nil {
			step.Status = "blocked"
		}
		record.Steps[len(record.Steps)-1] = step
		if e := os.WriteFile(filepath.Join(out, log), data, 0600); e != nil {
			return nil, e
		}
		if e := save(); e != nil {
			return nil, e
		}
		if err != nil {
			return nil, fmt.Errorf("%s failed; inspect %s", operation, log)
		}
		return data, nil
	}
	for _, endpoint := range []string{"source", "target"} {
		version := route.From
		if endpoint == "target" {
			version = route.To
		}
		selected, err := lock.selection(version)
		if err != nil {
			return err
		}
		root := filepath.Join(out, endpoint)
		if err := os.Mkdir(root, 0700); err != nil {
			return err
		}
		if selected.Release != nil {
			ref := selected.Release
			dir := releaseInputDir(config.Inputs, selected)
			resolved, err := command("verify "+endpoint+" product tag", "git", "-C", config.SourceRepository, "rev-parse", "--verify", "refs/tags/"+ref.Tag+"^{commit}")
			if err != nil {
				return err
			}
			if strings.TrimSpace(string(resolved)) != ref.Commit {
				return fmt.Errorf("%s product source tag differs from locked commit", endpoint)
			}
			if _, err := command("verify "+endpoint+" umbrella bundle", config.BundleVerifier, "umbrella", "verify-bundle", "--manifest", filepath.Join(dir, ref.Manifest.Name), "--bundle", filepath.Join(dir, ref.Bundle.Name), "--sha256", ref.Bundle.SHA256); err != nil {
				return err
			}
			if _, err := command("extract "+endpoint+" umbrella bundle", "tar", "-xzf", filepath.Join(dir, ref.Bundle.Name), "-C", root); err != nil {
				return err
			}
			record.ReleaseInputs[endpoint] = *ref
		}
		for _, plane := range planes {
			ref := selected.Stacks[plane]
			resolved, err := command("verify "+endpoint+" "+plane+" tag", "git", "-C", config.SourceRepository, "rev-parse", "--verify", "refs/tags/"+ref.Tag+"^{commit}")
			if err != nil {
				return err
			}
			if strings.TrimSpace(string(resolved)) != ref.Commit {
				return fmt.Errorf("%s %s source tag differs from locked commit", endpoint, plane)
			}
			path := "deploy/stacks/" + stackDirectories[plane]
			source := rehearsalSource{Endpoint: endpoint, Plane: plane, Ref: ref, SourceKind: selected.Kind}
			if selected.Release != nil {
				source.ArchiveSHA256 = selected.Release.Bundle.SHA256
			} else {
				archive := filepath.Join(out, "private", endpoint+"-"+plane+".tar")
				if _, err := command("archive "+endpoint+" "+plane, "git", "-C", config.SourceRepository, "archive", "--format=tar", "--output="+archive, ref.Commit, path); err != nil {
					return err
				}
				if _, err := command("extract "+endpoint+" "+plane, "tar", "-xf", archive, "-C", root); err != nil {
					return err
				}
				if source.ArchiveSHA256, err = fileChecksum(archive); err != nil {
					return err
				}
			}
			stack := filepath.Join(root, path)
			overlay := config.Overlays[endpoint][plane]
			for _, pair := range []struct {
				from, to string
				digest   *string
			}{{overlay.Values, "environments/rehearsal.yaml", &source.ValuesSHA256}, {overlay.Secrets, "secrets/rehearsal-secrets.yaml", &source.SecretsSHA256}} {
				data, err := os.ReadFile(pair.from)
				if err != nil {
					return err
				}
				if err := os.MkdirAll(filepath.Dir(filepath.Join(stack, pair.to)), 0700); err != nil {
					return err
				}
				f, err := os.OpenFile(filepath.Join(stack, pair.to), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err != nil {
					return err
				}
				_, writeErr := f.Write(data)
				closeErr := f.Close()
				if err := errors.Join(writeErr, closeErr); err != nil {
					return err
				}
				*pair.digest = checksum(data)
			}
			record.Sources = append(record.Sources, source)
			if err := save(); err != nil {
				return err
			}
		}
	}
	registrationDir := filepath.Join(out, "private", "registrations")
	if err := os.Mkdir(registrationDir, 0700); err != nil {
		return err
	}
	for _, registration := range config.ComputeRegistrations {
		raw, err := os.ReadFile(registration.Values)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(registrationDir, registration.ClusterName+"-register-values.yaml"), raw, 0600); err != nil {
			return err
		}
		record.Registrations[registration.ClusterName] = checksum(raw)
	}
	type renderJob struct {
		source               rehearsalSource
		registration         *rehearsalRegistration
		observabilityProfile string
	}
	var jobs []renderJob
	for _, source := range record.Sources {
		if source.Plane == "compute-plane" {
			for i := range config.ComputeRegistrations {
				jobs = append(jobs, renderJob{source: source, registration: &config.ComputeRegistrations[i]})
			}
		} else if source.Plane == "observability" {
			jobs = append(jobs, renderJob{source: source, observabilityProfile: "control"})
			for i := range config.ComputeRegistrations {
				jobs = append(jobs, renderJob{source: source, registration: &config.ComputeRegistrations[i], observabilityProfile: "compute"})
			}
		} else {
			jobs = append(jobs, renderJob{source: source})
		}
	}
	for _, job := range jobs {
		source := job.source
		stack := filepath.Join(out, source.Endpoint, "deploy/stacks", stackDirectories[source.Plane])
		// An empty, run-owned kubeconfig prevents use of the operator's default context.
		kubeconfig := filepath.Join(out, "private", "render-kubeconfig")
		if err := os.WriteFile(kubeconfig, []byte("apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []\n"), 0600); err != nil {
			return err
		}
		renderKey := source.Endpoint + "/" + source.Plane
		if job.observabilityProfile != "" {
			renderKey += "/" + job.observabilityProfile
		}
		environment := []string{"-C", stack, "HELMFILE_ENV=rehearsal", "KUBECONFIG=" + kubeconfig}
		if job.registration != nil {
			registration := job.registration
			renderKey += "/" + registration.ClusterName
			environment = append(environment, "CLUSTER_NAME="+registration.ClusterName, "OUTPUT_DIR="+registrationDir, "NCA_ID="+registration.NCAID)
		}
		renderDir := filepath.Join(out, "private", "renders", renderKey)
		if err := os.MkdirAll(renderDir, 0700); err != nil {
			return err
		}
		for _, setting := range []struct{ name, path string }{
			{"HELM_CACHE_HOME", "helm/cache"}, {"HELM_CONFIG_HOME", "helm/config"},
			{"HELM_DATA_HOME", "helm/data"}, {"HELMFILE_CACHE_HOME", "helmfile-cache"}, {"TMPDIR", "tmp"},
		} {
			path := filepath.Join(out, "private", setting.path)
			if err := os.MkdirAll(path, 0700); err != nil {
				return err
			}
			environment = append(environment, setting.name+"="+path)
		}
		environment = append(environment,
			"HELM_REPOSITORY_CONFIG="+filepath.Join(out, "private", "helm/config/repositories.yaml"),
			"HELM_REPOSITORY_CACHE="+filepath.Join(out, "private", "helm/cache/repository"))
		environment = append(environment, config.Helmfile, "--file", "helmfile.d", "--environment", "default", "--kubeconfig", kubeconfig)
		if job.observabilityProfile != "" {
			environment = append(environment, "--state-values-set", "observability.profile="+job.observabilityProfile)
		}
		environment = append(environment, "template", "--include-crds", "--output-dir", renderDir)
		_, err := command("render "+renderKey, "env", environment...)
		if err != nil {
			return err
		}
		digest, err := renderedTreeChecksum(renderDir)
		if err != nil {
			return fmt.Errorf("%s %s render: %w", source.Endpoint, source.Plane, err)
		}
		record.Renders[renderKey] = digest
	}
	if len(config.ComputeRegistrations) == 0 {
		return errComputeRegistrationsMissing
	}
	record.Status = "prepared"
	record.NextSafeAction = "Review effective values, rendered HA placement, enabled artifacts and migration order. Preparation does not authorize a baseline or upgrade."
	return nil
}
