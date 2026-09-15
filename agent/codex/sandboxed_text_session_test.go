package codex

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSandboxedTextThreadParamsAreEphemeralReadOnlyAndApprovalFree(t *testing.T) {
	workDir := filepath.Join(os.TempDir(), codexSandboxedTextDirPrefix+"params")
	session := &appServerSession{workDir: workDir, mode: codexSandboxedTextMode}
	params := session.threadRequestParams()

	if got := params["cwd"]; got != workDir {
		t.Fatalf("cwd = %#v, want %q", got, workDir)
	}
	if got := params["approvalPolicy"]; got != "never" {
		t.Fatalf("approvalPolicy = %#v, want never", got)
	}
	if got := params["sandbox"]; got != "read-only" {
		t.Fatalf("sandbox = %#v, want read-only", got)
	}
	if got := params["ephemeral"]; got != true {
		t.Fatalf("ephemeral = %#v, want true", got)
	}
	tools, ok := params["dynamicTools"].([]any)
	if !ok || len(tools) != 0 {
		t.Fatalf("dynamicTools = %#v, want empty array", params["dynamicTools"])
	}
	if got := params["developerInstructions"]; got != codexSandboxedTextInstructions {
		t.Fatalf("developerInstructions = %#v", got)
	}
}

func TestSandboxedTextWorkspaceCleanupRejectsUnexpectedPaths(t *testing.T) {
	workDir, err := newCodexSandboxedTextWorkDir()
	if err != nil {
		t.Fatalf("newCodexSandboxedTextWorkDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "probe.txt"), []byte("probe"), 0o600); err != nil {
		t.Fatalf("write probe: %v", err)
	}
	if err := removeCodexSandboxedTextWorkDir(workDir); err != nil {
		t.Fatalf("removeCodexSandboxedTextWorkDir: %v", err)
	}
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Fatalf("workspace still exists or stat failed unexpectedly: %v", err)
	}
	if err := removeCodexSandboxedTextWorkDir(os.TempDir()); err == nil {
		t.Fatal("cleanup accepted the broad temporary root")
	}
}

func TestSandboxedTextWorkspaceCleanupRetriesTransientWindowsHandle(t *testing.T) {
	workDir, err := newCodexSandboxedTextWorkDir()
	if err != nil {
		t.Fatalf("newCodexSandboxedTextWorkDir: %v", err)
	}

	originalRemoveAll := codexSandboxedTextRemoveAll
	originalTimeout := codexSandboxedTextCleanupTimeout
	originalInterval := codexSandboxedTextCleanupInterval
	t.Cleanup(func() {
		codexSandboxedTextRemoveAll = originalRemoveAll
		codexSandboxedTextCleanupTimeout = originalTimeout
		codexSandboxedTextCleanupInterval = originalInterval
		_ = os.RemoveAll(workDir)
	})

	attempts := 0
	codexSandboxedTextRemoveAll = func(path string) error {
		attempts++
		if attempts < 3 {
			return errors.New("workspace is still in use")
		}
		return os.RemoveAll(path)
	}
	codexSandboxedTextCleanupTimeout = time.Second
	codexSandboxedTextCleanupInterval = time.Millisecond

	if err := removeCodexSandboxedTextWorkDir(workDir); err != nil {
		t.Fatalf("removeCodexSandboxedTextWorkDir: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("cleanup attempts = %d, want 3", attempts)
	}
}
