package core

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultOpenAIGatewayListen     = "127.0.0.1:9840"
	defaultOpenAIGatewayTimeout    = 10 * time.Minute
	defaultOpenAISessionIdle       = 15 * time.Minute
	defaultOpenAIMaxSessions       = 16
	openAIModelDiscoveryTimeout    = 10 * time.Second
	openAIConversationResetTimeout = 30 * time.Second
	maxOpenAIRequestBody           = 4 << 20
	openAISessionModeHistory       = "history"
	openAISessionModeWarmReset     = "warm_reset"
)

// OpenAIGateway exposes cc-connect agents through a small, text-only subset of
// the OpenAI Chat Completions API. Requests are stateless unless clients opt in
// to history-preserving or warm-reset persistent runtimes.
type OpenAIGateway struct {
	listen    string
	token     string
	models    map[string]string
	timeout   time.Duration
	accessLog bool
	textOnly  bool

	mu      sync.RWMutex
	engines map[string]*Engine
	server  *http.Server
	ln      net.Listener

	persistentEnabled bool
	sessionIdle       time.Duration
	maxSessions       int
	sessionsMu        sync.Mutex
	sessions          map[string]*openAIPersistentSession
	janitorCancel     context.CancelFunc
	janitorDone       chan struct{}
}

type openAIPersistentSession struct {
	id              string
	project         string
	agentModel      string
	reasoningEffort string
	capabilityMode  string
	mode            string
	session         AgentSession
	cancel          context.CancelFunc
	history         []openAICanonicalMessage
	lastUsed        time.Time
	active          bool
}

type openAICanonicalMessage struct {
	Role    string
	Name    string
	Content string
}

type openAIGatewayTarget struct {
	PublicModel    string
	AgentModel     string
	Project        string
	Engine         *Engine
	CapabilityMode string
}

type openAIAccessLogContextKey struct{}

type openAIAccessLogFields struct {
	model           string
	project         string
	agentModel      string
	reasoningEffort string
	requestID       string
	sessionAction   string
	sessionID       string
	sessionMode     string
	contextReset    string
	capabilityMode  string
	stream          bool
}

type openAIAccessResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *openAIAccessResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *openAIAccessResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func (w *openAIAccessResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// NewOpenAIGateway creates a gateway. models maps public model IDs to
// cc-connect project names. When models is empty, every registered project is
// exposed as a model with the same name.
func NewOpenAIGateway(listen, token string, models map[string]string, timeout time.Duration) *OpenAIGateway {
	if strings.TrimSpace(listen) == "" {
		listen = defaultOpenAIGatewayListen
	}
	if timeout <= 0 {
		timeout = defaultOpenAIGatewayTimeout
	}
	modelCopy := make(map[string]string, len(models))
	for model, project := range models {
		model = strings.TrimSpace(model)
		project = strings.TrimSpace(project)
		if model != "" && project != "" {
			modelCopy[model] = project
		}
	}
	return &OpenAIGateway{
		listen:      listen,
		token:       token,
		models:      modelCopy,
		timeout:     timeout,
		engines:     make(map[string]*Engine),
		sessionIdle: defaultOpenAISessionIdle,
		maxSessions: defaultOpenAIMaxSessions,
		sessions:    make(map[string]*openAIPersistentSession),
	}
}

// ConfigurePersistentSessions enables the opt-in cc_session extension. A zero
// idle timeout or capacity selects the documented defaults.
func (g *OpenAIGateway) ConfigurePersistentSessions(enabled bool, idleTimeout time.Duration, maxSessions int) {
	if idleTimeout <= 0 {
		idleTimeout = defaultOpenAISessionIdle
	}
	if maxSessions <= 0 {
		maxSessions = defaultOpenAIMaxSessions
	}
	g.sessionsMu.Lock()
	g.persistentEnabled = enabled
	g.sessionIdle = idleTimeout
	g.maxSessions = maxSessions
	g.sessionsMu.Unlock()
}

// ConfigureAccessLogging enables one structured log record per HTTP request.
// Request bodies, response bodies, and authentication credentials are never
// included in these records.
func (g *OpenAIGateway) ConfigureAccessLogging(enabled bool) {
	g.accessLog = enabled
}

// ConfigureTextOnly makes the gateway fail closed: only agents that advertise
// and enforce zero-tool sessions are exposed, and every session receives a
// TextOnly requirement. This setting does not change the agents used by IM or
// direct CLI workflows.
func (g *OpenAIGateway) ConfigureTextOnly(enabled bool) {
	g.mu.Lock()
	g.textOnly = enabled
	g.mu.Unlock()
}

func (g *OpenAIGateway) RegisterEngine(name string, engine *Engine) {
	if engine == nil {
		return
	}
	g.mu.Lock()
	g.engines[name] = engine
	g.mu.Unlock()
}

func (g *OpenAIGateway) Start() error {
	if g.token == "" && !isLoopbackAddress(g.listen) {
		return fmt.Errorf("openai gateway: token is required for non-loopback listen address %q", g.listen)
	}

	ln, err := net.Listen("tcp", g.listen)
	if err != nil {
		return fmt.Errorf("openai gateway: listen on %s: %w", g.listen, err)
	}
	server := &http.Server{
		Handler:           g,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	g.mu.Lock()
	g.ln = ln
	g.server = server
	g.mu.Unlock()
	g.startPersistentSessionJanitor()

	go func() {
		slog.Info("openai gateway started", "listen", ln.Addr().String(), "models", g.modelIDs())
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("openai gateway server error", "error", err)
		}
	}()
	return nil
}

func (g *OpenAIGateway) startPersistentSessionJanitor() {
	g.sessionsMu.Lock()
	if !g.persistentEnabled || g.janitorCancel != nil {
		g.sessionsMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	g.janitorCancel = cancel
	g.janitorDone = make(chan struct{})
	done := g.janitorDone
	interval := g.sessionIdle / 2
	if interval < time.Second {
		interval = time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	g.sessionsMu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				g.pruneExpiredSessions(now)
			}
		}
	}()
}

func (g *OpenAIGateway) stopPersistentSessions() {
	g.sessionsMu.Lock()
	cancel := g.janitorCancel
	done := g.janitorDone
	g.janitorCancel = nil
	g.janitorDone = nil
	sessions := make([]*openAIPersistentSession, 0, len(g.sessions))
	for id, session := range g.sessions {
		delete(g.sessions, id)
		sessions = append(sessions, session)
	}
	g.sessionsMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	closeOpenAIPersistentSessions(sessions)
}

func (g *OpenAIGateway) Stop() {
	g.mu.RLock()
	server := g.server
	g.mu.RUnlock()
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := server.Shutdown(ctx); err != nil {
			slog.Warn("openai gateway shutdown failed", "error", err)
		}
		cancel()
	}
	g.stopPersistentSessions()
}

func (g *OpenAIGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	capabilityMode := g.capabilityMode()
	if capabilityMode != "" {
		w.Header().Set("X-CC-Capability-Mode", capabilityMode)
	}
	if g.accessLog {
		started := time.Now()
		access := &openAIAccessLogFields{sessionAction: "none", capabilityMode: capabilityMode}
		r = r.WithContext(context.WithValue(r.Context(), openAIAccessLogContextKey{}, access))
		recorder := &openAIAccessResponseWriter{ResponseWriter: w}
		w = recorder
		defer g.logAccess(r, recorder, access, started)
	}

	switch r.URL.Path {
	case "/health":
		if r.Method != http.MethodGet {
			writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "method_not_allowed", "")
			return
		}
		writeOpenAIJSON(w, http.StatusOK, map[string]any{"ok": true})
	case "/v1/models":
		if !g.authenticate(w, r) {
			return
		}
		g.handleModels(w, r)
	case "/v1/chat/completions":
		if !g.authenticate(w, r) {
			return
		}
		g.handleChatCompletions(w, r)
	default:
		writeOpenAIError(w, http.StatusNotFound, "route not found", "invalid_request_error", "not_found", "")
	}
}

func (g *OpenAIGateway) logAccess(r *http.Request, recorder *openAIAccessResponseWriter, access *openAIAccessLogFields, started time.Time) {
	status := recorder.status
	if status == 0 {
		status = http.StatusOK
	}
	attrs := []any{
		"method", r.Method,
		"path", r.URL.Path,
		"status", status,
		"duration_ms", time.Since(started).Milliseconds(),
		"bytes", recorder.bytes,
		"remote", openAIClientAddress(r.RemoteAddr),
	}
	if userAgent := strings.TrimSpace(r.UserAgent()); userAgent != "" {
		attrs = append(attrs, "user_agent", userAgent)
	}
	if access.model != "" {
		attrs = append(attrs, "model", access.model)
	}
	if access.project != "" {
		attrs = append(attrs, "project", access.project)
	}
	if access.agentModel != "" {
		attrs = append(attrs, "agent_model", access.agentModel)
	}
	if access.reasoningEffort != "" {
		attrs = append(attrs, "reasoning_effort", access.reasoningEffort)
	}
	if access.requestID != "" {
		attrs = append(attrs, "request_id", access.requestID)
	}
	if access.sessionAction != "none" {
		attrs = append(attrs, "session_action", access.sessionAction)
	}
	if access.sessionID != "" {
		attrs = append(attrs, "session_id", access.sessionID)
	}
	if access.sessionMode != "" {
		attrs = append(attrs, "session_mode", access.sessionMode)
	}
	if access.contextReset != "" {
		attrs = append(attrs, "context_reset", access.contextReset)
	}
	if access.capabilityMode != "" {
		attrs = append(attrs, "capability_mode", access.capabilityMode)
	}
	if access.stream {
		attrs = append(attrs, "stream", true)
	}
	slog.Info("openai gateway access", attrs...)
}

func openAIAccessFieldsFromContext(ctx context.Context) *openAIAccessLogFields {
	access, _ := ctx.Value(openAIAccessLogContextKey{}).(*openAIAccessLogFields)
	return access
}

func openAIClientAddress(remoteAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err == nil {
		return host
	}
	return strings.TrimSpace(remoteAddr)
}

func openAIResponseFlusher(w http.ResponseWriter) (http.Flusher, bool) {
	if recorder, ok := w.(*openAIAccessResponseWriter); ok {
		w = recorder.ResponseWriter
	}
	flusher, ok := w.(http.Flusher)
	return flusher, ok
}

func (g *OpenAIGateway) authenticate(w http.ResponseWriter, r *http.Request) bool {
	if g.token == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		writeOpenAIError(w, http.StatusUnauthorized, "invalid API key", "authentication_error", "invalid_api_key", "")
		return false
	}
	provided := strings.TrimSpace(auth[len(prefix):])
	if subtle.ConstantTimeCompare([]byte(provided), []byte(g.token)) != 1 {
		writeOpenAIError(w, http.StatusUnauthorized, "invalid API key", "authentication_error", "invalid_api_key", "")
		return false
	}
	return true
}

func (g *OpenAIGateway) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "method_not_allowed", "")
		return
	}
	models := g.availableModels(r.Context())
	writeOpenAIJSON(w, http.StatusOK, openAIModelList{Object: "list", Data: models})
}

func (g *OpenAIGateway) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "method_not_allowed", "")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxOpenAIRequestBody)
	defer r.Body.Close()

	var req openAIChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid JSON request body", "invalid_request_error", "invalid_json", "")
		return
	}
	access := openAIAccessFieldsFromContext(r.Context())
	if access != nil {
		access.model = strings.TrimSpace(req.Model)
		access.stream = req.Stream
		access.sessionAction = "stateless"
	}
	if strings.TrimSpace(req.Model) == "" {
		writeOpenAIError(w, http.StatusBadRequest, "model is required", "invalid_request_error", "missing_required_parameter", "model")
		return
	}
	if g.isTextOnlyUnsupportedModel(req.Model) {
		writeOpenAIError(w, http.StatusBadRequest, fmt.Sprintf("model %q is unavailable because its agent cannot enforce an isolated text-gateway session", strings.TrimSpace(req.Model)), "invalid_request_error", "text_only_unsupported", "model")
		return
	}
	target, err := g.resolveTarget(r.Context(), req.Model)
	if err != nil {
		writeOpenAIError(w, http.StatusNotFound, err.Error(), "invalid_request_error", "model_not_found", "model")
		return
	}
	if access != nil {
		access.project = target.Project
	}
	sessionOpts, invalidParam, err := validateOpenAISessionOptions(target, req.ReasoningEffort, g.isTextOnly())
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", "unsupported_value", invalidParam)
		return
	}
	if access != nil {
		access.reasoningEffort = sessionOpts.ReasoningEffort
		access.capabilityMode = sessionOpts.CapabilityMode
		access.agentModel = sessionOpts.Model
		if access.agentModel == "" {
			if switcher, ok := target.Engine.GetAgent().(ModelSwitcher); ok {
				access.agentModel = strings.TrimSpace(switcher.GetModel())
			}
		}
	}
	if sessionOpts.CapabilityMode != "" {
		w.Header().Set("X-CC-Capability-Mode", sessionOpts.CapabilityMode)
	}
	if req.N != 0 && req.N != 1 {
		writeOpenAIError(w, http.StatusBadRequest, "only n=1 is supported", "invalid_request_error", "unsupported_parameter", "n")
		return
	}
	if hasJSONValue(req.Tools) || hasJSONValue(req.ToolChoice) {
		writeOpenAIError(w, http.StatusBadRequest, "tool calling is not supported by this gateway", "invalid_request_error", "unsupported_parameter", "tools")
		return
	}
	if hasJSONValue(req.Functions) || hasJSONValue(req.FunctionCall) {
		writeOpenAIError(w, http.StatusBadRequest, "legacy function calling is not supported by this gateway", "invalid_request_error", "unsupported_parameter", "functions")
		return
	}
	if !isTextResponseFormat(req.ResponseFormat) {
		writeOpenAIError(w, http.StatusBadRequest, "structured response formats are not supported by this gateway", "invalid_request_error", "unsupported_parameter", "response_format")
		return
	}
	for _, modality := range req.Modalities {
		if modality != "text" {
			writeOpenAIError(w, http.StatusBadRequest, "only text output is supported by this gateway", "invalid_request_error", "unsupported_parameter", "modalities")
			return
		}
	}
	if hasJSONValue(req.Audio) {
		writeOpenAIError(w, http.StatusBadRequest, "audio output is not supported by this gateway", "invalid_request_error", "unsupported_parameter", "audio")
		return
	}
	id := newChatCompletionID()
	if access != nil {
		access.requestID = id
	}
	created := time.Now().Unix()
	ctx, cancel := context.WithTimeout(r.Context(), g.timeout)
	defer cancel()

	persistentRequested, requestedSessionID, sessionMode, err := requestedOpenAIPersistentSession(r, req)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", "unsupported_value", "cc_session_mode")
		return
	}
	if access != nil && persistentRequested {
		access.sessionMode = sessionMode
		if requestedSessionID == "" {
			access.sessionAction = "create"
		} else {
			access.sessionAction = "reuse"
			access.sessionID = requestedSessionID
		}
	}
	var persistentSession *openAIPersistentSession
	keepPersistentSession := false
	promptMessages := req.Messages
	var historyDelta []openAICanonicalMessage
	if persistentRequested {
		persistentSession, err = g.acquirePersistentSession(target, sessionOpts, requestedSessionID, sessionMode)
		if err != nil {
			status, code := persistentSessionErrorDetails(err)
			writeOpenAIError(w, status, err.Error(), "invalid_request_error", code, "cc_session_id")
			return
		}
		defer func() { g.releasePersistentSession(persistentSession, keepPersistentSession) }()
		if access != nil {
			access.sessionID = persistentSession.id
		}
		if sessionMode == openAISessionModeWarmReset {
			if err := validateOpenAIWarmResetMessages(req.Messages); err != nil {
				keepPersistentSession = true
				writeOpenAIError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", "warm_reset_requires_single_user_message", "messages")
				return
			}
		} else {
			promptMessages, historyDelta, err = persistentSession.promptDelta(req.Messages)
			if err != nil {
				keepPersistentSession = true
				writeOpenAIError(w, http.StatusConflict, err.Error(), "invalid_request_error", "session_history_mismatch", "messages")
				return
			}
		}
		if sessionMode != openAISessionModeWarmReset || req.Stream {
			w.Header().Set("X-CC-Session-ID", persistentSession.id)
		}
		w.Header().Set("X-CC-Session-Mode", sessionMode)
	}

	prompt, err := buildOpenAIChatPrompt(promptMessages)
	if err != nil {
		writeOpenAIError(w, http.StatusBadRequest, err.Error(), "invalid_request_error", "invalid_messages", "messages")
		return
	}

	if req.Stream {
		var afterCompletion func() error
		if persistentSession != nil && sessionMode == openAISessionModeWarmReset {
			afterCompletion = func() error {
				resetErr := g.resetPersistentConversation(persistentSession)
				if resetErr != nil {
					if access != nil {
						access.contextReset = "failed"
					}
					return resetErr
				}
				if access != nil {
					access.contextReset = "ok"
				}
				keepPersistentSession = true
				return nil
			}
		}
		result, streamErr := g.streamChatCompletion(ctx, w, target.Engine, req, prompt, id, created, sessionOpts, persistentSession, afterCompletion)
		if streamErr == nil && persistentSession != nil && sessionMode == openAISessionModeHistory {
			persistentSession.commitHistory(historyDelta, result.Text)
			keepPersistentSession = true
		}
		return
	}
	var result openAICompletionResult
	if persistentSession != nil {
		result, err = runOpenAICompletionWithSession(ctx, persistentSession.session, prompt, nil, nil)
	} else {
		result, err = runOpenAICompletion(ctx, target.Engine, prompt, nil, nil, sessionOpts)
	}
	if err != nil {
		writeOpenAICompletionFailure(w, err)
		return
	}
	contextReset := ""
	if persistentSession != nil {
		if sessionMode == openAISessionModeWarmReset {
			if resetErr := g.resetPersistentConversation(persistentSession); resetErr != nil {
				contextReset = "failed"
				slog.Warn("openai gateway: discard warm-reset runtime after reset failure", "session_id", persistentSession.id, "error", resetErr)
			} else {
				contextReset = "ok"
				keepPersistentSession = true
				w.Header().Set("X-CC-Session-ID", persistentSession.id)
			}
			if access != nil {
				access.contextReset = contextReset
			}
			w.Header().Set("X-CC-Context-Reset", contextReset)
		} else {
			persistentSession.commitHistory(historyDelta, result.Text)
			keepPersistentSession = true
		}
	}
	resp := openAIChatCompletionResponse{
		ID:               id,
		Object:           "chat.completion",
		Created:          created,
		Model:            req.Model,
		ReasoningEffort:  sessionOpts.ReasoningEffort,
		CCSessionMode:    sessionMode,
		CCContextReset:   contextReset,
		CCCapabilityMode: capabilityModeForOptions(sessionOpts),
		Choices: []openAIChatChoice{{
			Index:        0,
			Message:      openAIResponseMessage{Role: "assistant", Content: result.Text},
			FinishReason: "stop",
		}},
		Usage: result.Usage,
	}
	if persistentSession != nil && (sessionMode != openAISessionModeWarmReset || keepPersistentSession) {
		resp.CCSessionID = persistentSession.id
	}
	writeOpenAIJSON(w, http.StatusOK, resp)
}

func (g *OpenAIGateway) streamChatCompletion(ctx context.Context, w http.ResponseWriter, engine *Engine, req openAIChatCompletionRequest, prompt, id string, created int64, sessionOpts AgentSessionOptions, persistentSession *openAIPersistentSession, afterCompletion func() error) (openAICompletionResult, error) {
	flusher, ok := openAIResponseFlusher(w)
	if !ok {
		writeOpenAIError(w, http.StatusInternalServerError, "streaming is not supported by this HTTP server", "server_error", "streaming_unavailable", "stream")
		return openAICompletionResult{}, fmt.Errorf("streaming is not supported by this HTTP server")
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if err := writeOpenAIStreamChunk(w, flusher, openAIChatCompletionChunk{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model, ReasoningEffort: sessionOpts.ReasoningEffort, CCCapabilityMode: capabilityModeForOptions(sessionOpts), CCSessionID: persistentSessionID(persistentSession), CCSessionMode: persistentSessionMode(persistentSession),
		Choices: []openAIStreamChoice{{Index: 0, Delta: openAIStreamDelta{Role: "assistant"}}},
	}); err != nil {
		return openAICompletionResult{}, err
	}

	onText := func(text string) error {
		return writeOpenAIStreamChunk(w, flusher, openAIChatCompletionChunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model, ReasoningEffort: sessionOpts.ReasoningEffort, CCCapabilityMode: capabilityModeForOptions(sessionOpts),
			Choices: []openAIStreamChoice{{Index: 0, Delta: openAIStreamDelta{Content: text}}},
		})
	}
	var onThinking func(string) error
	if req.StreamOptions != nil && req.StreamOptions.IncludeReasoning {
		onThinking = func(text string) error {
			return writeOpenAIStreamChunk(w, flusher, openAIChatCompletionChunk{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model, ReasoningEffort: sessionOpts.ReasoningEffort, CCCapabilityMode: capabilityModeForOptions(sessionOpts),
				Choices: []openAIStreamChoice{{Index: 0, Delta: openAIStreamDelta{ReasoningContent: text}}},
			})
		}
	}
	var result openAICompletionResult
	var err error
	if persistentSession != nil {
		result, err = runOpenAICompletionWithSession(ctx, persistentSession.session, prompt, onText, onThinking)
	} else {
		result, err = runOpenAICompletion(ctx, engine, prompt, onText, onThinking, sessionOpts)
	}
	if err != nil {
		_ = writeOpenAIStreamData(w, flusher, openAIErrorEnvelope{Error: openAIErrorBody{
			Message: err.Error(), Type: "server_error", Code: "agent_error",
		}})
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		flusher.Flush()
		return openAICompletionResult{}, err
	}
	if afterCompletion != nil {
		if err := afterCompletion(); err != nil {
			_ = writeOpenAIStreamData(w, flusher, openAIErrorEnvelope{Error: openAIErrorBody{
				Message: err.Error(), Type: "server_error", Code: "context_reset_failed",
			}})
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			flusher.Flush()
			return result, err
		}
	}

	finish := "stop"
	contextReset := ""
	if persistentSession != nil && persistentSession.mode == openAISessionModeWarmReset {
		contextReset = "ok"
	}
	_ = writeOpenAIStreamChunk(w, flusher, openAIChatCompletionChunk{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model, ReasoningEffort: sessionOpts.ReasoningEffort, CCCapabilityMode: capabilityModeForOptions(sessionOpts), CCSessionID: persistentSessionID(persistentSession), CCSessionMode: persistentSessionMode(persistentSession), CCContextReset: contextReset,
		Choices: []openAIStreamChoice{{Index: 0, Delta: openAIStreamDelta{}, FinishReason: &finish}},
	})
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		_ = writeOpenAIStreamChunk(w, flusher, openAIChatCompletionChunk{
			ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model, ReasoningEffort: sessionOpts.ReasoningEffort, CCCapabilityMode: capabilityModeForOptions(sessionOpts),
			Choices: []openAIStreamChoice{}, Usage: &result.Usage,
		})
	}
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
	flusher.Flush()
	return result, nil
}

type openAICompletionResult struct {
	Text  string
	Usage openAIUsage
}

func capabilityModeForOptions(opts AgentSessionOptions) string {
	if opts.CapabilityMode != "" {
		return opts.CapabilityMode
	}
	if opts.TextOnly {
		return AgentCapabilityZeroTools
	}
	return ""
}

func runOpenAICompletion(ctx context.Context, engine *Engine, prompt string, onText, onThinking func(string) error, sessionOpts AgentSessionOptions) (openAICompletionResult, error) {
	agent := engine.GetAgent()
	if agent == nil {
		return openAICompletionResult{}, fmt.Errorf("project %q has no agent", engine.ProjectName())
	}
	var session AgentSession
	var err error
	if sessionOpts.Model != "" || sessionOpts.ReasoningEffort != "" || sessionOpts.TextOnly {
		starter, ok := agent.(SessionOptionsStarter)
		if !ok {
			return openAICompletionResult{}, fmt.Errorf("agent %q does not support per-request model or reasoning overrides", agent.Name())
		}
		session, err = starter.StartSessionWithOptions(ctx, "", sessionOpts)
	} else {
		session, err = agent.StartSession(ctx, "")
	}
	if err != nil {
		return openAICompletionResult{}, fmt.Errorf("start agent session: %w", err)
	}
	defer session.Close()
	return runOpenAICompletionWithSession(ctx, session, prompt, onText, onThinking)
}

func runOpenAICompletionWithSession(ctx context.Context, session AgentSession, prompt string, onText, onThinking func(string) error) (openAICompletionResult, error) {
	if err := session.Send(prompt, "", nil, nil); err != nil {
		return openAICompletionResult{}, fmt.Errorf("send prompt to agent: %w", err)
	}

	var result openAICompletionResult
	var text strings.Builder
	for {
		select {
		case <-ctx.Done():
			return openAICompletionResult{}, ctx.Err()
		case event, ok := <-session.Events():
			if !ok {
				if text.Len() == 0 {
					return openAICompletionResult{}, fmt.Errorf("agent session ended without a response")
				}
				result.Text = text.String()
				return result, nil
			}
			result.Usage = mergeOpenAIUsage(result.Usage, event)
			switch event.Type {
			case EventText:
				if event.Content == "" {
					continue
				}
				text.WriteString(event.Content)
				if onText != nil {
					if err := onText(event.Content); err != nil {
						return openAICompletionResult{}, err
					}
				}
			case EventThinking:
				if event.Content == "" || onThinking == nil {
					continue
				}
				if err := onThinking(event.Content); err != nil {
					return openAICompletionResult{}, err
				}
			case EventResult:
				if text.Len() == 0 && event.Content != "" {
					text.WriteString(event.Content)
					if onText != nil {
						if err := onText(event.Content); err != nil {
							return openAICompletionResult{}, err
						}
					}
				}
				result.Text = text.String()
				return result, nil
			case EventError:
				if event.Error != nil {
					return openAICompletionResult{}, event.Error
				}
				return openAICompletionResult{}, fmt.Errorf("agent returned an unspecified error")
			case EventPermissionRequest:
				_ = session.RespondPermission(event.RequestID, PermissionResult{
					Behavior: "deny",
					Message:  "interactive permissions are not supported through the OpenAI gateway",
				})
				return openAICompletionResult{}, fmt.Errorf("agent requested interactive permission for %s", event.ToolName)
			}
		}
	}
}

func (g *OpenAIGateway) resolveTarget(ctx context.Context, model string) (openAIGatewayTarget, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return openAIGatewayTarget{}, fmt.Errorf("model is required")
	}
	targets := g.gatewayTargets()
	for _, target := range targets {
		if target.PublicModel == model {
			return target, nil
		}
	}
	for _, target := range targets {
		prefix := target.PublicModel + "/"
		if strings.HasPrefix(model, prefix) && len(model) > len(prefix) {
			target.PublicModel = model
			target.AgentModel = strings.TrimSpace(strings.TrimPrefix(model, prefix))
			return target, nil
		}
	}
	// Preserve flat native model IDs when they identify exactly one project.
	// Namespaced IDs remain available for collisions and explicit routing.
	if len(targets) == 1 {
		target := targets[0]
		target.PublicModel = model
		target.AgentModel = model
		return target, nil
	}
	matches := make(map[string]openAIGatewayTarget)
	for _, target := range targets {
		agent := target.Engine.GetAgent()
		switcher, ok := agent.(ModelSwitcher)
		if !ok {
			continue
		}
		discoveryCtx, cancel := context.WithTimeout(ctx, openAIModelDiscoveryTimeout)
		options := switcher.AvailableModels(discoveryCtx)
		cancel()
		for _, option := range options {
			if strings.TrimSpace(option.Name) != model {
				continue
			}
			target.PublicModel = model
			target.AgentModel = model
			matches[target.Project] = target
			break
		}
	}
	if len(matches) == 1 {
		for _, target := range matches {
			return target, nil
		}
	}
	if len(matches) > 1 {
		return openAIGatewayTarget{}, fmt.Errorf("model %q is ambiguous; use a project/model ID from /v1/models", model)
	}
	return openAIGatewayTarget{}, fmt.Errorf("model %q does not exist; use a project/model ID from /v1/models", model)
}

type openAIPersistentSessionError struct {
	status int
	code   string
	msg    string
}

func (e *openAIPersistentSessionError) Error() string { return e.msg }

func persistentSessionError(status int, code, format string, args ...any) error {
	return &openAIPersistentSessionError{status: status, code: code, msg: fmt.Sprintf(format, args...)}
}

func persistentSessionErrorDetails(err error) (int, string) {
	if sessionErr, ok := err.(*openAIPersistentSessionError); ok {
		return sessionErr.status, sessionErr.code
	}
	return http.StatusInternalServerError, "persistent_session_error"
}

func requestedOpenAIPersistentSession(r *http.Request, req openAIChatCompletionRequest) (bool, string, string, error) {
	sessionID := strings.TrimSpace(req.CCSessionID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(r.Header.Get("X-CC-Session-ID"))
	}
	forceNew := strings.EqualFold(sessionID, "new")
	if forceNew {
		sessionID = ""
	}
	mode := strings.ToLower(strings.TrimSpace(req.CCSessionMode))
	requested := req.CCSession || forceNew || sessionID != "" || mode != ""
	if !requested {
		return false, "", "", nil
	}
	if mode == "" {
		mode = openAISessionModeHistory
	}
	if mode != openAISessionModeHistory && mode != openAISessionModeWarmReset {
		return false, "", "", fmt.Errorf("cc_session_mode %q is not supported; available values: %s, %s", req.CCSessionMode, openAISessionModeHistory, openAISessionModeWarmReset)
	}
	return true, sessionID, mode, nil
}

func persistentSessionID(session *openAIPersistentSession) string {
	if session == nil {
		return ""
	}
	return session.id
}

func (g *OpenAIGateway) acquirePersistentSession(target openAIGatewayTarget, opts AgentSessionOptions, requestedID, mode string) (*openAIPersistentSession, error) {
	now := time.Now()
	g.pruneExpiredSessions(now)

	if requestedID != "" {
		g.sessionsMu.Lock()
		if !g.persistentEnabled {
			g.sessionsMu.Unlock()
			return nil, persistentSessionError(http.StatusBadRequest, "persistent_sessions_disabled", "persistent sessions are disabled")
		}
		session := g.sessions[requestedID]
		if session == nil {
			g.sessionsMu.Unlock()
			return nil, persistentSessionError(http.StatusNotFound, "session_not_found", "persistent session %q was not found or has expired", requestedID)
		}
		if session.project != target.Project || session.agentModel != opts.Model || session.reasoningEffort != opts.ReasoningEffort || session.capabilityMode != opts.CapabilityMode || session.mode != mode {
			g.sessionsMu.Unlock()
			return nil, persistentSessionError(http.StatusConflict, "session_options_mismatch", "persistent session %q is bound to its original project, model, reasoning_effort, capability mode, and session mode", requestedID)
		}
		if session.active {
			g.sessionsMu.Unlock()
			return nil, persistentSessionError(http.StatusConflict, "session_busy", "persistent session %q is already processing a request", requestedID)
		}
		if session.session == nil || !session.session.Alive() {
			delete(g.sessions, requestedID)
			g.sessionsMu.Unlock()
			closeOpenAIPersistentSessions([]*openAIPersistentSession{session})
			return nil, persistentSessionError(http.StatusGone, "session_closed", "persistent session %q is no longer alive", requestedID)
		}
		session.active = true
		session.lastUsed = now
		g.sessionsMu.Unlock()
		return session, nil
	}

	g.sessionsMu.Lock()
	if !g.persistentEnabled {
		g.sessionsMu.Unlock()
		return nil, persistentSessionError(http.StatusBadRequest, "persistent_sessions_disabled", "persistent sessions are disabled")
	}
	var evicted *openAIPersistentSession
	if len(g.sessions) >= g.maxSessions {
		for _, candidate := range g.sessions {
			if candidate.active || evicted != nil && !candidate.lastUsed.Before(evicted.lastUsed) {
				continue
			}
			evicted = candidate
		}
		if evicted == nil {
			g.sessionsMu.Unlock()
			return nil, persistentSessionError(http.StatusTooManyRequests, "session_capacity_exceeded", "all %d persistent session slots are busy", g.maxSessions)
		}
		delete(g.sessions, evicted.id)
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &openAIPersistentSession{
		id:              newOpenAIPersistentSessionID(),
		project:         target.Project,
		agentModel:      opts.Model,
		reasoningEffort: opts.ReasoningEffort,
		capabilityMode:  opts.CapabilityMode,
		mode:            mode,
		cancel:          cancel,
		lastUsed:        now,
		active:          true,
	}
	g.sessions[session.id] = session
	g.sessionsMu.Unlock()
	closeOpenAIPersistentSessions([]*openAIPersistentSession{evicted})

	agent := target.Engine.GetAgent()
	if agent == nil {
		g.releasePersistentSession(session, false)
		return nil, persistentSessionError(http.StatusInternalServerError, "agent_unavailable", "project %q has no agent", target.Project)
	}
	var agentSession AgentSession
	var err error
	if mode == openAISessionModeWarmReset {
		starter, ok := agent.(IsolatedSessionStarter)
		if !ok {
			err = fmt.Errorf("agent %q does not support isolated warm-reset sessions", agent.Name())
		} else {
			agentSession, err = starter.StartIsolatedSession(sessionCtx, "", opts)
			if err == nil {
				if _, ok := agentSession.(ConversationResetter); !ok {
					err = fmt.Errorf("agent %q returned an isolated session without conversation reset support", agent.Name())
				}
			}
		}
	} else if starter, ok := agent.(PersistentSessionStarter); ok {
		agentSession, err = starter.StartPersistentSession(sessionCtx, "", opts)
	} else if opts.Model != "" || opts.ReasoningEffort != "" || opts.TextOnly {
		starter, ok := agent.(SessionOptionsStarter)
		if !ok {
			err = fmt.Errorf("agent %q does not support persistent sessions with request options", agent.Name())
		} else {
			agentSession, err = starter.StartSessionWithOptions(sessionCtx, "", opts)
		}
	} else {
		agentSession, err = agent.StartSession(sessionCtx, "")
	}
	if err == nil && agentSession == nil {
		err = fmt.Errorf("agent %q returned an empty persistent session", agent.Name())
	}
	if err != nil {
		g.releasePersistentSession(session, false)
		return nil, persistentSessionError(http.StatusInternalServerError, "session_start_failed", "start persistent agent session: %v", err)
	}
	g.sessionsMu.Lock()
	if g.sessions[session.id] != session {
		g.sessionsMu.Unlock()
		_ = agentSession.Close()
		cancel()
		return nil, persistentSessionError(http.StatusServiceUnavailable, "gateway_stopping", "persistent session creation was interrupted by gateway shutdown")
	}
	session.session = agentSession
	g.sessionsMu.Unlock()
	return session, nil
}

func persistentSessionMode(session *openAIPersistentSession) string {
	if session == nil {
		return ""
	}
	return session.mode
}

func (g *OpenAIGateway) resetPersistentConversation(session *openAIPersistentSession) error {
	if session == nil || session.session == nil {
		return fmt.Errorf("warm-reset session is unavailable")
	}
	resetter, ok := session.session.(ConversationResetter)
	if !ok {
		return fmt.Errorf("persistent session does not support conversation reset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), openAIConversationResetTimeout)
	defer cancel()
	if err := resetter.ResetConversation(ctx); err != nil {
		return fmt.Errorf("reset agent conversation: %w", err)
	}
	return nil
}

func validateOpenAIWarmResetMessages(messages []openAIChatMessage) error {
	if len(messages) != 1 {
		return fmt.Errorf("warm_reset requires exactly one self-contained user message")
	}
	if !strings.EqualFold(strings.TrimSpace(messages[0].Role), "user") {
		return fmt.Errorf("warm_reset requires messages[0].role to be user")
	}
	canonical, err := canonicalizeOpenAIChatMessages(messages)
	if err != nil {
		return err
	}
	if len(canonical) != 1 || strings.TrimSpace(canonical[0].Content) == "" {
		return fmt.Errorf("warm_reset requires a non-empty user message")
	}
	return nil
}

func (g *OpenAIGateway) releasePersistentSession(session *openAIPersistentSession, keep bool) {
	if session == nil {
		return
	}
	g.sessionsMu.Lock()
	current := g.sessions[session.id]
	if current != session {
		g.sessionsMu.Unlock()
		return
	}
	if keep && session.session != nil && session.session.Alive() {
		session.active = false
		session.lastUsed = time.Now()
		g.sessionsMu.Unlock()
		return
	}
	delete(g.sessions, session.id)
	session.active = false
	g.sessionsMu.Unlock()
	closeOpenAIPersistentSessions([]*openAIPersistentSession{session})
}

func (g *OpenAIGateway) pruneExpiredSessions(now time.Time) {
	g.sessionsMu.Lock()
	idleTimeout := g.sessionIdle
	expired := make([]*openAIPersistentSession, 0)
	for id, session := range g.sessions {
		if !session.active && now.Sub(session.lastUsed) >= idleTimeout {
			delete(g.sessions, id)
			expired = append(expired, session)
		}
	}
	g.sessionsMu.Unlock()
	closeOpenAIPersistentSessions(expired)
}

func closeOpenAIPersistentSessions(sessions []*openAIPersistentSession) {
	for _, session := range sessions {
		if session == nil {
			continue
		}
		if session.session != nil {
			if err := session.session.Close(); err != nil {
				slog.Warn("openai gateway: close persistent session failed", "session_id", session.id, "error", err)
			}
		}
		if session.cancel != nil {
			session.cancel()
		}
	}
}

func (s *openAIPersistentSession) promptDelta(messages []openAIChatMessage) ([]openAIChatMessage, []openAICanonicalMessage, error) {
	canonical, err := canonicalizeOpenAIChatMessages(messages)
	if err != nil {
		return nil, nil, err
	}
	if len(s.history) == 0 {
		return messages, canonical, nil
	}
	if len(canonical) >= len(s.history) && canonicalMessagePrefix(canonical, s.history) {
		if len(canonical) == len(s.history) {
			return nil, nil, fmt.Errorf("persistent session request does not contain a new message")
		}
		return messages[len(s.history):], canonical[len(s.history):], nil
	}
	if len(canonical) == 1 {
		return messages, canonical, nil
	}
	return nil, nil, fmt.Errorf("messages do not extend the history of persistent session %q; start a new session or send only the new message", s.id)
}

func (s *openAIPersistentSession) commitHistory(delta []openAICanonicalMessage, assistantText string) {
	s.history = append(s.history, delta...)
	s.history = append(s.history, openAICanonicalMessage{Role: "assistant", Content: assistantText})
}

func canonicalizeOpenAIChatMessages(messages []openAIChatMessage) ([]openAICanonicalMessage, error) {
	canonical := make([]openAICanonicalMessage, 0, len(messages))
	for i, message := range messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		switch role {
		case "developer", "system", "user", "assistant", "tool", "function":
		default:
			return nil, fmt.Errorf("messages[%d].role %q is not supported", i, message.Role)
		}
		if hasJSONValue(message.ToolCalls) {
			return nil, fmt.Errorf("messages[%d].tool_calls is not supported", i)
		}
		content, err := parseOpenAIMessageContent(message.Content)
		if err != nil {
			return nil, fmt.Errorf("messages[%d].content: %w", i, err)
		}
		canonical = append(canonical, openAICanonicalMessage{Role: role, Name: message.Name, Content: content})
	}
	return canonical, nil
}

func canonicalMessagePrefix(messages, prefix []openAICanonicalMessage) bool {
	if len(messages) < len(prefix) {
		return false
	}
	for i := range prefix {
		if messages[i] != prefix[i] {
			return false
		}
	}
	return true
}

func newOpenAIPersistentSessionID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("ccs_%d", time.Now().UnixNano())
	}
	return "ccs_" + hex.EncodeToString(buf)
}

func (g *OpenAIGateway) gatewayTargets() []openAIGatewayTarget {
	g.mu.RLock()
	defer g.mu.RUnlock()
	targets := make([]openAIGatewayTarget, 0)
	if len(g.models) > 0 {
		for publicModel, project := range g.models {
			if engine := g.engines[project]; engine != nil {
				capabilityMode := ""
				if g.textOnly {
					capabilityMode = gatewayTextCapabilityMode(engine.GetAgent())
				}
				if g.textOnly && capabilityMode == "" {
					continue
				}
				targets = append(targets, openAIGatewayTarget{PublicModel: publicModel, Project: project, Engine: engine, CapabilityMode: capabilityMode})
			}
		}
	} else {
		for project, engine := range g.engines {
			if engine != nil {
				capabilityMode := ""
				if g.textOnly {
					capabilityMode = gatewayTextCapabilityMode(engine.GetAgent())
				}
				if g.textOnly && capabilityMode == "" {
					continue
				}
				targets = append(targets, openAIGatewayTarget{PublicModel: project, Project: project, Engine: engine, CapabilityMode: capabilityMode})
			}
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].PublicModel < targets[j].PublicModel })
	return targets
}

func supportsTextOnlySessions(agent Agent) bool {
	capability, ok := agent.(TextOnlySessionCapable)
	return ok && capability.SupportsTextOnlySessions()
}

func supportsSandboxedTextSessions(agent Agent) bool {
	capability, ok := agent.(SandboxedTextSessionCapable)
	return ok && capability.SupportsSandboxedTextSessions()
}

func gatewayTextCapabilityMode(agent Agent) string {
	if supportsTextOnlySessions(agent) {
		return AgentCapabilityZeroTools
	}
	if supportsSandboxedTextSessions(agent) {
		return AgentCapabilitySandboxedText
	}
	return ""
}

func (g *OpenAIGateway) isTextOnly() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.textOnly
}

func (g *OpenAIGateway) capabilityMode() string {
	if g.isTextOnly() {
		return "text_only"
	}
	return ""
}

// isTextOnlyUnsupportedModel distinguishes an explicitly configured but
// filtered model from an unknown native model. Without this guard, the
// single-target native-model fallback could accidentally route it elsewhere.
func (g *OpenAIGateway) isTextOnlyUnsupportedModel(model string) bool {
	model = strings.TrimSpace(model)
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.textOnly || model == "" {
		return false
	}
	if len(g.models) > 0 {
		for publicModel, project := range g.models {
			if model != publicModel && !strings.HasPrefix(model, publicModel+"/") {
				continue
			}
			engine := g.engines[project]
			return engine != nil && gatewayTextCapabilityMode(engine.GetAgent()) == ""
		}
		return false
	}
	for project, engine := range g.engines {
		if model != project && !strings.HasPrefix(model, project+"/") {
			continue
		}
		return engine != nil && gatewayTextCapabilityMode(engine.GetAgent()) == ""
	}
	return false
}

func (g *OpenAIGateway) availableModels(ctx context.Context) []openAIModel {
	targets := g.gatewayTargets()
	singleProject := len(targets) == 1
	byID := make(map[string]openAIModel)
	type nativeModel struct {
		target     openAIGatewayTarget
		agentModel string
		efforts    []string
	}
	var nativeModels []nativeModel
	nativeProjects := make(map[string]map[string]struct{})
	for _, target := range targets {
		agent := target.Engine.GetAgent()
		if agent == nil {
			continue
		}
		efforts := availableReasoningEfforts(agent)
		defaultModel := ""
		if switcher, ok := agent.(ModelSwitcher); ok {
			defaultModel = strings.TrimSpace(switcher.GetModel())
		}
		byID[target.PublicModel] = openAIModel{
			ID: target.PublicModel, Object: "model", Created: 0, OwnedBy: "cc-connect",
			Project: target.Project, AgentModel: defaultModel, ReasoningEfforts: efforts,
			CCCapabilityMode: target.CapabilityMode,
		}

		switcher, ok := agent.(ModelSwitcher)
		if !ok {
			continue
		}
		discoveryCtx, cancel := context.WithTimeout(ctx, openAIModelDiscoveryTimeout)
		options := switcher.AvailableModels(discoveryCtx)
		cancel()
		for _, option := range options {
			agentModel := strings.TrimSpace(option.Name)
			if agentModel == "" {
				continue
			}
			nativeModels = append(nativeModels, nativeModel{target: target, agentModel: agentModel, efforts: efforts})
			if nativeProjects[agentModel] == nil {
				nativeProjects[agentModel] = make(map[string]struct{})
			}
			nativeProjects[agentModel][target.Project] = struct{}{}
		}
	}
	for _, native := range nativeModels {
		publicModels := []string{native.agentModel}
		if !singleProject {
			publicModels = []string{native.target.PublicModel + "/" + native.agentModel}
			if len(nativeProjects[native.agentModel]) == 1 {
				publicModels = append(publicModels, native.agentModel)
			}
		}
		for _, publicModel := range publicModels {
			if _, exists := byID[publicModel]; exists {
				continue
			}
			byID[publicModel] = openAIModel{
				ID: publicModel, Object: "model", Created: 0, OwnedBy: "cc-connect",
				Project: native.target.Project, AgentModel: native.agentModel, ReasoningEfforts: native.efforts,
				CCCapabilityMode: native.target.CapabilityMode,
			}
		}
	}
	models := make([]openAIModel, 0, len(byID))
	for _, model := range byID {
		models = append(models, model)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models
}

func availableReasoningEfforts(agent Agent) []string {
	switcher, ok := agent.(ReasoningEffortSwitcher)
	if !ok {
		return nil
	}
	raw := switcher.AvailableReasoningEfforts()
	efforts := make([]string, 0, len(raw))
	for _, effort := range raw {
		effort = strings.ToLower(strings.TrimSpace(effort))
		if effort != "" {
			efforts = append(efforts, effort)
		}
	}
	return efforts
}

func validateOpenAISessionOptions(target openAIGatewayTarget, reasoningEffort string, textOnly bool) (AgentSessionOptions, string, error) {
	agent := target.Engine.GetAgent()
	if agent == nil {
		return AgentSessionOptions{}, "model", fmt.Errorf("project %q has no agent", target.Project)
	}
	opts := AgentSessionOptions{Model: strings.TrimSpace(target.AgentModel)}
	if textOnly {
		opts.TextOnly = true
		opts.CapabilityMode = target.CapabilityMode
		if opts.CapabilityMode == "" {
			return AgentSessionOptions{}, "model", fmt.Errorf("agent %q cannot enforce a text-gateway isolation mode", agent.Name())
		}
	}
	if opts.Model != "" {
		if _, ok := agent.(ModelSwitcher); !ok {
			return AgentSessionOptions{}, "model", fmt.Errorf("agent %q does not support model selection", agent.Name())
		}
	}
	effort := strings.ToLower(strings.TrimSpace(reasoningEffort))
	if effort != "" {
		allowed := availableReasoningEfforts(agent)
		valid := false
		for _, candidate := range allowed {
			if candidate == effort {
				valid = true
				break
			}
		}
		if !valid {
			return AgentSessionOptions{}, "reasoning_effort", fmt.Errorf("reasoning_effort %q is not supported; available values: %s", reasoningEffort, strings.Join(allowed, ", "))
		}
		opts.ReasoningEffort = effort
	}
	if opts.Model != "" || opts.ReasoningEffort != "" || opts.TextOnly {
		if _, ok := agent.(SessionOptionsStarter); !ok {
			return AgentSessionOptions{}, "model", fmt.Errorf("agent %q does not support isolated per-request overrides", agent.Name())
		}
	}
	return opts, "", nil
}

func (g *OpenAIGateway) modelIDs() []string {
	targets := g.gatewayTargets()
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.PublicModel)
	}
	sort.Strings(ids)
	return ids
}

func buildOpenAIChatPrompt(messages []openAIChatMessage) (string, error) {
	if len(messages) == 0 {
		return "", fmt.Errorf("messages must contain at least one item")
	}
	var b strings.Builder
	b.WriteString("Answer the following chat conversation. Follow developer and system instructions, preserve the conversation context, and return only the assistant response.\n\n")
	written := 0
	for i, message := range messages {
		role := strings.ToLower(strings.TrimSpace(message.Role))
		switch role {
		case "developer", "system", "user", "assistant", "tool", "function":
		default:
			return "", fmt.Errorf("messages[%d].role %q is not supported", i, message.Role)
		}
		if hasJSONValue(message.ToolCalls) {
			return "", fmt.Errorf("messages[%d].tool_calls is not supported", i)
		}
		content, err := parseOpenAIMessageContent(message.Content)
		if err != nil {
			return "", fmt.Errorf("messages[%d].content: %w", i, err)
		}
		if strings.TrimSpace(content) == "" {
			continue
		}
		label := strings.ToUpper(role)
		if message.Name != "" {
			label += " " + message.Name
		}
		fmt.Fprintf(&b, "[%s]\n%s\n\n", label, content)
		written++
	}
	if written == 0 {
		return "", fmt.Errorf("messages do not contain any text content")
	}
	b.WriteString("[ASSISTANT]\n")
	return b.String(), nil
}

func parseOpenAIMessageContent(raw json.RawMessage) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var parts []openAIContentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("must be a string or an array of text parts")
	}
	var b strings.Builder
	for _, part := range parts {
		switch part.Type {
		case "text", "input_text", "output_text":
			b.WriteString(part.Text)
		default:
			return "", fmt.Errorf("content part type %q is not supported", part.Type)
		}
	}
	return b.String(), nil
}

func mergeOpenAIUsage(current openAIUsage, event Event) openAIUsage {
	if event.InputTokens > 0 {
		current.PromptTokens = event.InputTokens
	}
	if event.OutputTokens > 0 {
		current.CompletionTokens = event.OutputTokens
	}
	current.TotalTokens = current.PromptTokens + current.CompletionTokens
	return current
}

func writeOpenAICompletionFailure(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "agent_error"
	if err == context.DeadlineExceeded {
		status = http.StatusGatewayTimeout
		code = "timeout"
	} else if err == context.Canceled {
		status = http.StatusRequestTimeout
		code = "request_canceled"
	}
	writeOpenAIError(w, status, err.Error(), "server_error", code, "")
}

func writeOpenAIStreamChunk(w io.Writer, flusher http.Flusher, chunk openAIChatCompletionChunk) error {
	return writeOpenAIStreamData(w, flusher, chunk)
}

func writeOpenAIStreamData(w io.Writer, flusher http.Flusher, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

func writeOpenAIError(w http.ResponseWriter, status int, message, errorType, code, param string) {
	writeOpenAIJSON(w, status, openAIErrorEnvelope{Error: openAIErrorBody{
		Message: message,
		Type:    errorType,
		Param:   param,
		Code:    code,
	}})
}

func writeOpenAIJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Debug("openai gateway: write JSON failed", "error", err)
	}
}

func hasJSONValue(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null" && trimmed != "[]" && trimmed != "{}" && trimmed != `"none"`
}

func isTextResponseFormat(raw json.RawMessage) bool {
	if !hasJSONValue(raw) {
		return true
	}
	var format struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &format); err != nil {
		return false
	}
	return format.Type == "text"
}

func newChatCompletionID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	}
	return "chatcmpl-" + hex.EncodeToString(buf)
}

func isLoopbackAddress(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type openAIChatCompletionRequest struct {
	Model           string               `json:"model"`
	ReasoningEffort string               `json:"reasoning_effort,omitempty"`
	CCSession       bool                 `json:"cc_session,omitempty"`
	CCSessionID     string               `json:"cc_session_id,omitempty"`
	CCSessionMode   string               `json:"cc_session_mode,omitempty"`
	Messages        []openAIChatMessage  `json:"messages"`
	Stream          bool                 `json:"stream,omitempty"`
	StreamOptions   *openAIStreamOptions `json:"stream_options,omitempty"`
	N               int                  `json:"n,omitempty"`
	Tools           json.RawMessage      `json:"tools,omitempty"`
	ToolChoice      json.RawMessage      `json:"tool_choice,omitempty"`
	Functions       json.RawMessage      `json:"functions,omitempty"`
	FunctionCall    json.RawMessage      `json:"function_call,omitempty"`
	ResponseFormat  json.RawMessage      `json:"response_format,omitempty"`
	Modalities      []string             `json:"modalities,omitempty"`
	Audio           json.RawMessage      `json:"audio,omitempty"`
}

type openAIChatMessage struct {
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	Name      string          `json:"name,omitempty"`
	ToolCalls json.RawMessage `json:"tool_calls,omitempty"`
}

type openAIContentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type openAIStreamOptions struct {
	IncludeUsage     bool `json:"include_usage,omitempty"`
	IncludeReasoning bool `json:"include_reasoning,omitempty"`
}

type openAIChatCompletionResponse struct {
	ID               string             `json:"id"`
	Object           string             `json:"object"`
	Created          int64              `json:"created"`
	Model            string             `json:"model"`
	ReasoningEffort  string             `json:"reasoning_effort,omitempty"`
	CCCapabilityMode string             `json:"cc_capability_mode,omitempty"`
	CCSessionID      string             `json:"cc_session_id,omitempty"`
	CCSessionMode    string             `json:"cc_session_mode,omitempty"`
	CCContextReset   string             `json:"cc_context_reset,omitempty"`
	Choices          []openAIChatChoice `json:"choices"`
	Usage            openAIUsage        `json:"usage"`
}

type openAIChatChoice struct {
	Index        int                   `json:"index"`
	Message      openAIResponseMessage `json:"message"`
	FinishReason string                `json:"finish_reason"`
}

type openAIResponseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIChatCompletionChunk struct {
	ID               string               `json:"id"`
	Object           string               `json:"object"`
	Created          int64                `json:"created"`
	Model            string               `json:"model"`
	ReasoningEffort  string               `json:"reasoning_effort,omitempty"`
	CCCapabilityMode string               `json:"cc_capability_mode,omitempty"`
	CCSessionID      string               `json:"cc_session_id,omitempty"`
	CCSessionMode    string               `json:"cc_session_mode,omitempty"`
	CCContextReset   string               `json:"cc_context_reset,omitempty"`
	Choices          []openAIStreamChoice `json:"choices"`
	Usage            *openAIUsage         `json:"usage,omitempty"`
}

type openAIStreamChoice struct {
	Index        int               `json:"index"`
	Delta        openAIStreamDelta `json:"delta"`
	FinishReason *string           `json:"finish_reason"`
}

type openAIStreamDelta struct {
	Role             string `json:"role,omitempty"`
	Content          string `json:"content,omitempty"`
	ReasoningContent string `json:"reasoning_content,omitempty"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type openAIModelList struct {
	Object string        `json:"object"`
	Data   []openAIModel `json:"data"`
}

type openAIModel struct {
	ID               string   `json:"id"`
	Object           string   `json:"object"`
	Created          int64    `json:"created"`
	OwnedBy          string   `json:"owned_by"`
	Project          string   `json:"cc_connect_project,omitempty"`
	AgentModel       string   `json:"cc_connect_agent_model,omitempty"`
	ReasoningEfforts []string `json:"reasoning_efforts,omitempty"`
	CCCapabilityMode string   `json:"cc_capability_mode,omitempty"`
}

type openAIErrorEnvelope struct {
	Error openAIErrorBody `json:"error"`
}

type openAIErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param,omitempty"`
	Code    string `json:"code,omitempty"`
}
