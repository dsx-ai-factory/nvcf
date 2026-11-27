// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func umbrellaDocsStageFixture(t *testing.T) (string, string, editionRelease) {
	t.Helper()
	root, catalog, legacy := editionGitFixture(t)
	if err := registerEdition(root, editionRelease{Version: "1.0.0", Ref: "docs/releases/1.0.0", Commit: legacy}, false); err != nil {
		t.Fatal(err)
	}
	m := umbrellaTestManifest()
	m.SourceCommit = legacy
	m.Status = ReleaseSetQualified
	m.Qualification = catalog.DocsEdition.Qualification
	for _, name := range releaseSetStackNames {
		stack := m.Stacks[name]
		selected, _ := catalog.ReleaseSet.Stacks.byName(name)
		stack.SourceCommit = selected.SourceCommit
		m.Stacks[name] = stack
	}
	data, err := marshalUmbrellaManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, umbrellaManifestPath(m.Version)), string(data))
	editionGit(t, root, "add", ".")
	editionGit(t, root, "commit", "-m", "product")
	product := editionGit(t, root, "rev-parse", "HEAD")
	editionGit(t, root, "tag", umbrellaTag(m.Version))
	release := editionRelease{Version: m.Version, Ref: "docs/versions/" + m.Version, Commit: legacy, Mode: "umbrella", Umbrella: &umbrellaDocsBinding{ProductCommit: product, ManifestSHA256: umbrellaDigest(data), Revisions: []string{legacy}}}
	source := filepath.Join(t.TempDir(), "source")
	if err := prepareUmbrellaDocs(root, source, release, false); err != nil {
		t.Fatal(err)
	}
	editionGit(t, source, "config", "user.name", "Fixture")
	editionGit(t, source, "config", "user.email", "fixture@example.com")
	writeFile(t, filepath.Join(source, "docs/page.md"), "# Approved product docs\n")
	editionGit(t, source, "add", ".")
	editionGit(t, source, "commit", "-m", "approved docs")
	release.Commit = editionGit(t, source, "rev-parse", "HEAD")
	release.Umbrella.Revisions = []string{release.Commit}
	editionGit(t, source, "push", "origin", "HEAD:refs/heads/"+release.Ref)
	if err := reviseUmbrellaDocs(root, release, false); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "fern/fern.config.json"), "{\"version\":\"5.144.1\"}\n")
	writeFile(t, filepath.Join(root, "fern/docs.yml"), `title: Fixture
versions:
  - display-name: '1.0.0'
    slug: '1.0.0'
    ref: docs/releases/1.0.0
  - display-name: '1.0.3'
    slug: '1.0.3'
    ref: docs/versions/1.0.3
  - display-name: Development
    slug: dev
    path: navigation.yml
redirects:
  - source: /old
    destination: /new
`)
	return root, source, release
}

func TestUmbrellaCanonicalDocsStageE2E(t *testing.T) {
	root, source, release := umbrellaDocsStageFixture(t)
	writeFile(t, filepath.Join(root, "docs/page.md"), "# Working-tree preview edit\n")
	out := filepath.Join(t.TempDir(), "build")
	if err := stageCanonicalDocs(root, out, false); err != nil {
		t.Fatal(err)
	}
	preview, err := os.ReadFile(filepath.Join(out, "docs/page.md"))
	if err != nil || string(preview) != "# Working-tree preview edit\n" {
		t.Fatalf("preview staging lost working-tree edits: %v %s", err, preview)
	}
	// Advance after staging, then verify validation/generation inputs remain frozen.
	writeFile(t, filepath.Join(source, "docs/page.md"), "# Unapproved branch content\n")
	editionGit(t, source, "add", ".")
	editionGit(t, source, "commit", "-m", "later edit")
	editionGit(t, source, "push", "origin", "HEAD:refs/heads/"+release.Ref)
	path := filepath.Join(out, "fern/versions", release.Version, "docs/page.md")
	text, err := os.ReadFile(path)
	if err != nil || string(text) != "# Approved product docs\n" {
		t.Fatalf("staging selected wrong source: %v %s", err, text)
	}
	data, err := os.ReadFile(filepath.Join(out, "fern/docs.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	versions := config["versions"].([]any)
	if len(versions) != 3 || len(config["redirects"].([]any)) != 1 {
		t.Fatal("lost selectors or redirects")
	}
	for i, version := range versions {
		entry := version.(map[string]any)
		if entry["ref"] != nil {
			t.Fatal("staged configuration still resolves mutable refs")
		}
		if i == 2 && entry["path"] != "navigation.yml" {
			t.Fatal("changed Development source")
		}
	}
	if _, err := os.ReadFile(filepath.Join(out, "fern/versions/1.0.0/docs/shared.md")); err != nil {
		t.Fatalf("legacy symlink lost: %v", err)
	}
	receipt, err := os.ReadFile(filepath.Join(out, "docs-build-receipt.json"))
	if err != nil || !bytes.Contains(receipt, []byte(release.Commit)) || !bytes.Contains(receipt, []byte(release.Umbrella.ManifestSHA256)) {
		t.Fatal("missing approved source receipt")
	}
	// Rebuilding the same approved canonical configuration is the rollback path.
	again := filepath.Join(t.TempDir(), "again")
	if err := stageCanonicalDocs(root, again, false); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := os.ReadFile(filepath.Join(again, "fern/versions", release.Version, "docs/page.md"))
	if err != nil || !bytes.Equal(text, rebuilt) {
		t.Fatal("rollback rebuild changed approved text")
	}
	// Missing approved content must not create a publishable staged tree.
	editionGit(t, root, "branch", "-f", release.Ref, release.Umbrella.ProductCommit)
	rejected := filepath.Join(t.TempDir(), "rejected")
	if err := stageCanonicalDocs(root, rejected, false); err == nil {
		t.Fatal("accepted missing approval")
	}
	if _, err := os.Stat(rejected); !os.IsNotExist(err) {
		t.Fatal("failed staging left publishable output")
	}
}

func TestCanonicalDocsStageLegacyUnchanged(t *testing.T) {
	root, _, commit := editionGitFixture(t)
	if err := registerEdition(root, editionRelease{Version: "1.0.0", Ref: "docs/releases/1.0.0", Commit: commit}, false); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "fern/fern.config.json"), "{\"version\":\"5.144.1\"}\n")
	original, err := os.ReadFile(filepath.Join(root, "fern/docs.yml"))
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "build")
	if err := stageCanonicalDocs(root, out, false); err != nil {
		t.Fatal(err)
	}
	staged, err := os.ReadFile(filepath.Join(out, "fern/docs.yml"))
	if err != nil || !bytes.Equal(staged, original) {
		t.Fatal("legacy-only navigation was changed")
	}
}

func TestUmbrellaDocsProductAssetReadback(t *testing.T) {
	for _, schema := range []int{1, 2} {
		t.Run("schema-"+strconv.Itoa(schema), func(t *testing.T) {
			testUmbrellaDocsProductAssetReadback(t, schema)
		})
	}
}

func testUmbrellaDocsProductAssetReadback(t *testing.T, schema int) {
	fixture := umbrellaBundleFixture(t)
	args := fixture.prepareArgs()
	if schema == 2 {
		args = fixture.releaseLocalArgs()
	}
	if err := runUmbrella(args[1:]); err != nil {
		t.Fatal(err)
	}
	manifest, _, err := loadUmbrellaManifest(fixture.manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Status = ReleaseSetQualified
	manifest.Qualification = "https://github.com/dsx-ai-factory/nvcf/issues/2483"
	data, err := marshalUmbrellaManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "assets")
	if err := assembleUmbrellaBundle(fixture.root, fixture.inventories, directory, manifest, data, nil); err != nil {
		t.Fatal(err)
	}
	_, assets, err := readUmbrellaReleaseAssets(directory)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(fixture.root, umbrellaManifestPath(manifest.Version)), string(data))
	editionGit(t, fixture.root, "add", ".")
	editionGit(t, fixture.root, "commit", "-m", "product")
	product := editionGit(t, fixture.root, "rev-parse", "HEAD")
	editionGit(t, fixture.root, "tag", umbrellaTag(manifest.Version))
	release := editionRelease{Version: manifest.Version, Ref: "docs/versions/" + manifest.Version, Commit: product, Mode: "umbrella", Umbrella: &umbrellaDocsBinding{ProductCommit: product, ManifestSHA256: umbrellaDigest(data), Revisions: []string{product}}}
	names := []string{}
	for name := range assets {
		names = append(names, name)
	}
	sort.Strings(names)
	remote := umbrellaRelease{ID: 1, Tag: umbrellaTag(manifest.Version)}
	unavailable := false
	serverURL := ""
	// downloadURLs overrides the public download URL the release listing reports.
	downloadURLs := map[string]string{}
	downloadPrefix := "/dsx-ai-factory/nvcf/releases/download/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/repos/dsx-ai-factory/nvcf")
		if r.Method != "GET" || r.Header.Get("Authorization") != "" {
			t.Error("docs artifact check must be public and read-only")
		}
		if strings.HasPrefix(r.URL.Path, downloadPrefix) {
			if unavailable {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			name := strings.TrimPrefix(r.URL.Path, downloadPrefix+remote.Tag+"/")
			content, ok := assets[name]
			if !ok {
				w.WriteHeader(404)
				return
			}
			w.Write(content)
			return
		}
		switch {
		case strings.HasPrefix(path, "/git/ref/tags/"):
			json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"type": "commit", "sha": product}})
		case strings.HasPrefix(path, "/releases/tags/"):
			json.NewEncoder(w).Encode(remote)
		case path == "/releases/1/assets":
			list := []umbrellaReleaseAsset{}
			for i, name := range names {
				if _, ok := assets[name]; ok {
					url, overridden := downloadURLs[name]
					if !overridden {
						url = serverURL + downloadPrefix + remote.Tag + "/" + name
					}
					list = append(list, umbrellaReleaseAsset{ID: int64(i + 10), Name: name, State: "uploaded", BrowserDownloadURL: url})
				}
			}
			json.NewEncoder(w).Encode(list)
		case strings.HasPrefix(path, "/releases/assets/"):
			// The authenticated-style API download is the request that GitHub
			// rejects with HTTP 403 for public release assets (issue 3713).
			t.Errorf("docs product read must use the public download URL, not %s", r.URL.Path)
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	serverURL = server.URL
	publisher := umbrellaPublisher{api: server.URL, repository: "dsx-ai-factory/nvcf", client: server.Client()}
	readback, err := readPublishedDocsProduct(fixture.root, release, publisher)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range assets {
		if !bytes.Equal(readback[name], want) {
			t.Fatalf("public readback changed original asset %s", name)
		}
	}
	for _, state := range []string{"draft", "prerelease"} {
		t.Run(state, func(t *testing.T) {
			remote.Draft, remote.Prerelease = state == "draft", state == "prerelease"
			if err := verifyPublishedDocsProduct(fixture.root, release, publisher); err == nil || !strings.Contains(err.Error(), "publicly stable") {
				t.Fatalf("accepted nonpublic product: %v", err)
			}
		})
	}
	remote.Draft, remote.Prerelease = false, false
	unavailable = true
	if err := verifyPublishedDocsProduct(fixture.root, release, publisher); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("accepted unavailable product assets: %v", err)
	}
	unavailable = false
	t.Run("download-url-must-name-the-release-asset", func(t *testing.T) {
		name := names[0]
		for label, url := range map[string]string{
			"missing":     "",
			"other-asset": serverURL + downloadPrefix + remote.Tag + "/" + names[len(names)-1],
			"other-tag":   serverURL + downloadPrefix + "nvcf/9.9.9/" + name,
			"other-repo":  serverURL + "/other/repo/releases/download/" + remote.Tag + "/" + name,
			"credentials": strings.Replace(serverURL, "http://", "http://user:pass@", 1) + downloadPrefix + remote.Tag + "/" + name,
		} {
			downloadURLs[name] = url
			if err := verifyPublishedDocsProduct(fixture.root, release, publisher); err == nil || !strings.Contains(err.Error(), "public download URL") {
				t.Errorf("%s: accepted download URL %q: %v", label, url, err)
			}
		}
		delete(downloadURLs, name)
	})
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			original := assets[name]
			delete(assets, name)
			if err := verifyPublishedDocsProduct(fixture.root, release, publisher); err == nil || !strings.Contains(err.Error(), "missing") {
				t.Fatalf("missing public asset accepted: %v", err)
			}
			assets[name] = append(append([]byte(nil), original...), 1)
			if err := verifyPublishedDocsProduct(fixture.root, release, publisher); err == nil {
				t.Fatal("changed public asset accepted")
			}
			assets[name] = original
		})
	}
}
