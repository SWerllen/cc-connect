//go:build windows

package codex

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func TestAppServerCloseKillsWindowsChildProcessTree(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	script := fmt.Sprintf(
		`$child = Start-Process -FilePath ping.exe -ArgumentList '127.0.0.1','-n','120' -PassThru; Set-Content -LiteralPath %s -Value $child.Id; Wait-Process -Id $child.Id`,
		powerShellTestLiteral(pidFile),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script)
	prepareCmdForKill(cmd)
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start process tree: %v", err)
	}

	childPID := waitForChildPIDFile(t, pidFile)
	t.Cleanup(func() {
		cancel()
		_, _ = exec.Command("taskkill", "/F", "/PID", strconv.Itoa(childPID)).CombinedOutput()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})

	session := &appServerSession{
		cmd:    cmd,
		ctx:    ctx,
		cancel: cancel,
		events: make(chan core.Event, 1),
	}
	session.alive.Store(true)
	session.wg.Add(1)
	go func() {
		defer session.wg.Done()
		_ = cmd.Wait()
	}()

	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if waitForWindowsProcessExit(childPID, 3*time.Second) {
		t.Fatalf("child process %d survived app-server Close", childPID)
	}
}

func waitForChildPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if convErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("child PID file was not created: %s", path)
	return 0
}

func waitForWindowsProcessExit(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		output, _ := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH").CombinedOutput()
		if !strings.Contains(string(output), strconv.Itoa(pid)) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

func powerShellTestLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
