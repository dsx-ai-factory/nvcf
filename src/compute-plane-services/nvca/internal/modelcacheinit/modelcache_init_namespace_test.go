package modelcacheinit

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Repro for nvbugs#7055457: on the NVCA 3.7.x staging clusters the namespace
// nvca-modelcache-init does not exist, so the model cache controller fails every
// lease creation with `namespaces "nvca-modelcache-init" not found` and helm
// tasks/functions carrying a model or resource never start. The deploy
// artifacts this repository ships for NVCA are the nearest repository-owned
// point where that namespace can be provisioned, so this test renders them and
// asserts the namespace is created.

const initNamespace = "nvca-modelcache-init"

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 12; i++ {
		if _, statErr := os.Stat(filepath.Join(dir, ".git")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("could not locate repository root from the test working directory")
	return ""
}

func nvcaCharts(root string) []string {
	var charts []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if info.Name() != "Chart.yaml" {
			return nil
		}
		dir := filepath.Dir(path)
		if strings.Contains(strings.ToLower(dir), "nvca") {
			charts = append(charts, dir)
		}
		return nil
	})
	return charts
}

func declaresInitNamespace(manifests string) bool {
	for _, doc := range strings.Split(manifests, "\n---") {
		if !strings.Contains(doc, "kind: Namespace") {
			continue
		}
		if strings.Contains(doc, initNamespace) {
			return true
		}
	}
	return false
}

func TestNVCAChartsProvisionModelCacheInitNamespace(t *testing.T) {
	helmBin, err := exec.LookPath("helm")
	if err != nil {
		t.Fatalf("helm not on PATH, cannot render NVCA's own deploy charts: %v", err)
	}
	root := repoRoot(t)
	charts := nvcaCharts(root)
	if len(charts) == 0 {
		t.Fatalf("no NVCA helm chart found under %s: this repository ships nothing that creates the %q namespace the model cache controller requires", root, initNamespace)
	}
	var rendered strings.Builder
	var failures []string
	for _, chart := range charts {
		out, runErr := exec.Command(helmBin, "template", "nvca", chart, "--namespace", "nvca-system").CombinedOutput()
		if runErr != nil {
			failures = append(failures, chart+": "+runErr.Error()+"\n"+string(out))
			continue
		}
		rendered.WriteString("# chart: " + chart + "\n")
		rendered.Write(out)
		rendered.WriteString("\n")
	}
	if declaresInitNamespace(rendered.String()) {
		return
	}
	t.Fatalf("none of the %d NVCA charts %v render a Namespace named %q: nothing this repository deploys provisions the model cache init namespace, which is why the controller loops on `namespaces %q not found` and helm tasks/functions with a model or resource never start.\nrender failures:\n%s", len(charts), charts, initNamespace, initNamespace, strings.Join(failures, "\n"))
}
