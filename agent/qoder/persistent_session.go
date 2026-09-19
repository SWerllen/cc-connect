package qoder

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/google/uuid"
)

// qoderPersistentSession uses Qoder's structured streaming input protocol.
// Unlike qoderSession, the qodercli process remains alive after a result and
// accepts the next user message on stdin.
type qoderPersistentSession struct {
	base     *qoderSession
	cmd      *exec.Cmd
	isolated bool

	writeMu sync.Mutex
	stdin   io.WriteCloser
	stderr  bytes.Buffer

	closeOnce sync.Once

	resetMu           sync.Mutex
	lastUserMessageID string
	pendingResets     map[string]chan qoderRewindResponse
}

type qoderRewindResponse struct {
	Subtype string
	Status  string
	Error   string
}

type qoderControlResponseEnvelope struct {
	Type     string `json:"type"`
	Response struct {
		Subtype   string `json:"subtype"`
		RequestID string `json:"request_id"`
		Error     string `json:"error"`
		Response  struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"response"`
	} `json:"response"`
}

type qoderSDKUserMessage struct {
	Type            string          `json:"type"`
	Message         qoderSDKMessage `json:"message"`
	ParentToolUseID any             `json:"parent_tool_use_id"`
	UUID            string          `json:"uuid"`
}

type qoderSDKMessage struct {
	Role    string            `json:"role"`
	Content []qoderSDKContent `json:"content"`
}

type qoderSDKContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func newQoderPersistentSession(ctx context.Context, cmd string, extraArgs []string, workDir, model, reasoningEffort, mode, resumeID string, extraEnv []string, isolated, textOnly bool) (*qoderPersistentSession, error) {
	base, err := newQoderSession(ctx, cmd, extraArgs, workDir, model, reasoningEffort, mode, resumeID, extraEnv, textOnly)
	if err != nil {
		return nil, err
	}
	session := &qoderPersistentSession{
		base:          base,
		isolated:      isolated,
		pendingResets: make(map[string]chan qoderRewindResponse),
	}
	if err := session.start(); err != nil {
		base.cancel()
		return nil, err
	}
	return session, nil
}

func (s *qoderPersistentSession) start() error {
	qs := s.base
	args := append(append([]string{}, qs.extraArgs...),
		"--print",
		"--output-format", "stream-json",
		"--input-format", "stream-json",
		"--cwd", qs.workDir,
	)
	args = appendTextOnlyArgs(args, qs.textOnly)
	if sid := qs.CurrentSessionID(); sid != "" {
		args = append(args, "--resume", sid)
	}
	if qs.mode == "yolo" {
		if os.Geteuid() == 0 {
			slog.Warn("qoderPersistentSession: --dangerously-skip-permissions not allowed under root, skipping flag")
		} else {
			args = append(args, "--dangerously-skip-permissions")
		}
	}
	if qs.model != "" {
		args = append(args, "--model", qs.model)
	}
	if qs.reasoningEffort != "" {
		args = append(args, "--reasoning-effort", qs.reasoningEffort)
	}

	cmd := exec.CommandContext(qs.ctx, qs.cmd, args...)
	cmd.Dir = qs.workDir
	if len(qs.extraEnv) > 0 {
		cmd.Env = core.MergeEnv(os.Environ(), qs.extraEnv)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("qoderPersistentSession: stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return fmt.Errorf("qoderPersistentSession: stdout pipe: %w", err)
	}
	cmd.Stderr = &s.stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return fmt.Errorf("qoderPersistentSession: start: %w", err)
	}
	s.cmd = cmd
	s.stdin = stdin
	qs.wg.Add(1)
	go s.readLoop(stdout)
	slog.Info("qoder persistent session started", "pid", cmd.Process.Pid, "work_dir", qs.workDir)
	return nil
}

func (s *qoderPersistentSession) Send(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if len(images) > 0 {
		slog.Warn("qoderPersistentSession: images are not supported, ignoring")
	}
	if len(files) > 0 {
		filePaths := core.SaveFilesToDisk(s.base.workDir, messageID, files)
		prompt = core.AppendFileRefs(prompt, filePaths)
	}
	if !s.Alive() {
		return fmt.Errorf("qoderPersistentSession: session is closed")
	}
	userMessageID := uuid.NewString()
	payload := qoderSDKUserMessage{
		Type: "user",
		Message: qoderSDKMessage{
			Role:    "user",
			Content: []qoderSDKContent{{Type: "text", Text: prompt}},
		},
		ParentToolUseID: nil,
		UUID:            userMessageID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("qoderPersistentSession: encode prompt: %w", err)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.stdin == nil {
		return fmt.Errorf("qoderPersistentSession: stdin is closed")
	}
	if _, err := s.stdin.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("qoderPersistentSession: write prompt: %w", err)
	}
	s.resetMu.Lock()
	s.lastUserMessageID = userMessageID
	s.resetMu.Unlock()
	return nil
}

func (s *qoderPersistentSession) readLoop(stdout io.ReadCloser) {
	defer s.base.wg.Done()
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if s.handleControlResponse([]byte(line)) {
			continue
		}
		var event streamEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			slog.Debug("qoderPersistentSession: non-JSON line", "line", truncStr(line, 100))
			continue
		}
		s.base.handleEvent(&event)
	}
	scanErr := scanner.Err()
	exitErr := s.cmd.Wait()
	s.base.alive.Store(false)
	if s.base.ctx.Err() != nil {
		return
	}
	err := scanErr
	if err == nil {
		err = exitErr
	}
	if err == nil {
		err = fmt.Errorf("qoder persistent process exited unexpectedly")
	}
	stderr := truncStr(s.stderr.String(), 500)
	if stderr != "" {
		err = fmt.Errorf("%w: %s", err, stderr)
	}
	s.rejectPendingResets(err)
	select {
	case s.base.events <- core.Event{Type: core.EventError, Error: err}:
	case <-s.base.ctx.Done():
	}
}

func (s *qoderPersistentSession) RespondPermission(requestID string, result core.PermissionResult) error {
	return s.base.RespondPermission(requestID, result)
}

func (s *qoderPersistentSession) Events() <-chan core.Event { return s.base.Events() }

func (s *qoderPersistentSession) CurrentSessionID() string { return s.base.CurrentSessionID() }

func (s *qoderPersistentSession) Alive() bool { return s.base.Alive() }

// ResetConversation removes the completed turn from Qoder's active model
// conversation while keeping the bidirectional qodercli process alive. The
// conversation-only scope deliberately leaves workspace files untouched.
func (s *qoderPersistentSession) ResetConversation(ctx context.Context) error {
	if !s.isolated {
		return fmt.Errorf("qoder persistent session was not started in isolated mode")
	}
	if !s.Alive() {
		return fmt.Errorf("qoder persistent session is closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.resetMu.Lock()
	userMessageID := s.lastUserMessageID
	s.resetMu.Unlock()
	if userMessageID == "" {
		return fmt.Errorf("qoder conversation rewind has no completed user turn")
	}

	var response qoderRewindResponse
	for attempt := 0; ; attempt++ {
		var err error
		response, err = s.rewindConversation(ctx, userMessageID)
		if err != nil {
			return err
		}
		if response.Subtype == "success" && response.Status == "success" {
			break
		}
		if attempt >= 9 || !isQoderRewindBusy(response) {
			return fmt.Errorf("qoder conversation rewind rejected: subtype=%q status=%q error=%q", response.Subtype, response.Status, response.Error)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}

	s.base.textMu.Lock()
	s.base.assistantTextByID = make(map[string]string)
	s.base.assistantTextOrder = nil
	s.base.thinkingTextByID = make(map[string]string)
	s.base.thinkingTextOrder = nil
	s.base.textMu.Unlock()
	s.resetMu.Lock()
	if s.lastUserMessageID == userMessageID {
		s.lastUserMessageID = ""
	}
	s.resetMu.Unlock()
	return nil
}

func (s *qoderPersistentSession) rewindConversation(ctx context.Context, userMessageID string) (qoderRewindResponse, error) {
	requestID := "cc-reset-" + uuid.NewString()
	payload := map[string]any{
		"type":       "control_request",
		"request_id": requestID,
		"request": map[string]any{
			"subtype":         "rewind",
			"user_message_id": userMessageID,
			"scope":           "conversation",
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return qoderRewindResponse{}, fmt.Errorf("qoderPersistentSession: encode rewind: %w", err)
	}
	responseCh := make(chan qoderRewindResponse, 1)
	s.resetMu.Lock()
	s.pendingResets[requestID] = responseCh
	s.resetMu.Unlock()

	removePending := func() {
		s.resetMu.Lock()
		delete(s.pendingResets, requestID)
		s.resetMu.Unlock()
	}
	s.writeMu.Lock()
	if s.stdin == nil {
		s.writeMu.Unlock()
		removePending()
		return qoderRewindResponse{}, fmt.Errorf("qoderPersistentSession: stdin is closed")
	}
	_, err = s.stdin.Write(append(data, '\n'))
	s.writeMu.Unlock()
	if err != nil {
		removePending()
		return qoderRewindResponse{}, fmt.Errorf("qoderPersistentSession: write rewind: %w", err)
	}

	select {
	case response := <-responseCh:
		return response, nil
	case <-ctx.Done():
		removePending()
		return qoderRewindResponse{}, ctx.Err()
	case <-s.base.ctx.Done():
		removePending()
		return qoderRewindResponse{}, fmt.Errorf("qoderPersistentSession: session closed while waiting for rewind")
	}
}

func (s *qoderPersistentSession) handleControlResponse(line []byte) bool {
	var envelope qoderControlResponseEnvelope
	if err := json.Unmarshal(line, &envelope); err != nil || envelope.Type != "control_response" {
		return false
	}
	response := qoderRewindResponse{
		Subtype: envelope.Response.Subtype,
		Status:  envelope.Response.Response.Status,
		Error:   strings.TrimSpace(envelope.Response.Response.Error),
	}
	if response.Error == "" {
		response.Error = strings.TrimSpace(envelope.Response.Error)
	}
	s.resetMu.Lock()
	responseCh := s.pendingResets[envelope.Response.RequestID]
	delete(s.pendingResets, envelope.Response.RequestID)
	s.resetMu.Unlock()
	if responseCh != nil {
		responseCh <- response
	}
	return true
}

func (s *qoderPersistentSession) rejectPendingResets(err error) {
	message := "qoder persistent process exited"
	if err != nil {
		message = err.Error()
	}
	s.resetMu.Lock()
	pending := s.pendingResets
	s.pendingResets = make(map[string]chan qoderRewindResponse)
	s.resetMu.Unlock()
	for _, responseCh := range pending {
		responseCh <- qoderRewindResponse{Subtype: "error", Status: "error", Error: message}
	}
}

func isQoderRewindBusy(response qoderRewindResponse) bool {
	text := strings.ToLower(response.Status + " " + response.Error)
	return strings.Contains(text, "idle") || strings.Contains(text, "busy") || strings.Contains(text, "queued") || strings.Contains(text, "in progress")
}

func (s *qoderPersistentSession) Close() error {
	s.closeOnce.Do(func() {
		s.base.alive.Store(false)
		s.writeMu.Lock()
		if s.stdin != nil {
			_ = s.stdin.Close()
			s.stdin = nil
		}
		s.writeMu.Unlock()
		s.base.cancel()
		done := make(chan struct{})
		go func() {
			s.base.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			close(s.base.events)
		case <-time.After(8 * time.Second):
			slog.Warn("qoderPersistentSession: close timed out")
		}
	})
	return nil
}
