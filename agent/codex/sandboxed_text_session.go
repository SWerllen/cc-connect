package codex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const (
	codexSandboxedTextMode         = "sandboxed-text"
	codexSandboxedTextDirPrefix    = "cc-connect-codex-sandboxed-text-"
	codexSandboxedTextInstructions = "You are serving a sandboxed text-only inference request. Return text only. Do not call tools, inspect files, run commands, access the network, modify state, or ask for approval. All task context is contained in the user message."
)

var (
	codexSandboxedTextCleanupTimeout  = 5 * time.Second
	codexSandboxedTextCleanupInterval = 100 * time.Millisecond
	codexSandboxedTextRemoveAll       = os.RemoveAll
)

type codexSandboxedTextSession struct {
	core.AgentSession
	workDir   string
	closeOnce sync.Once
	closeErr  error
}

func newCodexSandboxedTextSession(session core.AgentSession, workDir string) core.AgentSession {
	return &codexSandboxedTextSession{AgentSession: session, workDir: workDir}
}

func (s *codexSandboxedTextSession) ResetConversation(ctx context.Context) error {
	resetter, ok := s.AgentSession.(core.ConversationResetter)
	if !ok {
		return fmt.Errorf("codex sandboxed text session does not support conversation reset")
	}
	return resetter.ResetConversation(ctx)
}

func (s *codexSandboxedTextSession) StartupWarning() string {
	warner, _ := s.AgentSession.(core.StartupWarner)
	if warner == nil {
		return ""
	}
	return warner.StartupWarning()
}

func (s *codexSandboxedTextSession) Close() error {
	s.closeOnce.Do(func() {
		closeErr := s.AgentSession.Close()
		cleanupErr := removeCodexSandboxedTextWorkDir(s.workDir)
		s.closeErr = errors.Join(closeErr, cleanupErr)
	})
	return s.closeErr
}

func newCodexSandboxedTextWorkDir() (string, error) {
	workDir, err := os.MkdirTemp("", codexSandboxedTextDirPrefix)
	if err != nil {
		return "", fmt.Errorf("codex: create sandboxed text workspace: %w", err)
	}
	return workDir, nil
}

func removeCodexSandboxedTextWorkDir(workDir string) error {
	workDir = strings.TrimSpace(workDir)
	if workDir == "" {
		return nil
	}
	absoluteDir, err := filepath.Abs(workDir)
	if err != nil {
		return fmt.Errorf("codex: resolve sandboxed text workspace: %w", err)
	}
	tempRoot, err := filepath.Abs(os.TempDir())
	if err != nil {
		return fmt.Errorf("codex: resolve temporary directory: %w", err)
	}
	relative, err := filepath.Rel(tempRoot, absoluteDir)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || !strings.HasPrefix(filepath.Base(absoluteDir), codexSandboxedTextDirPrefix) {
		return fmt.Errorf("codex: refuse to remove unexpected sandboxed text workspace %q", absoluteDir)
	}
	deadline := time.Now().Add(codexSandboxedTextCleanupTimeout)
	var cleanupErr error
	for {
		cleanupErr = codexSandboxedTextRemoveAll(absoluteDir)
		if cleanupErr == nil || os.IsNotExist(cleanupErr) {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("codex: remove sandboxed text workspace after retry: %w", cleanupErr)
		}
		time.Sleep(codexSandboxedTextCleanupInterval)
	}
}
