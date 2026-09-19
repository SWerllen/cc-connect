package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type gatewayTestAgent struct {
	mu               sync.Mutex
	prompts          []string
	events           []Event
	models           []ModelOption
	defaultModel     string
	reasoningEfforts []string
	sessionOptions   []AgentSessionOptions
	startCount       int
	persistentStarts int
	isolatedStarts   int
	resetCount       int
	resetErr         error
	closeCount       int
}

type gatewayTextOnlyTestAgent struct {
	*gatewayTestAgent
}

func (a *gatewayTextOnlyTestAgent) SupportsTextOnlySessions() bool { return true }

type gatewaySandboxedTextTestAgent struct {
	*gatewayTestAgent
}

func (a *gatewaySandboxedTextTestAgent) SupportsSandboxedTextSessions() bool { return true }

func (a *gatewayTestAgent) Name() string { return "gateway-test" }

func (a *gatewayTestAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.mu.Lock()
	a.startCount++
	a.mu.Unlock()
	return &gatewayTestSession{agent: a, events: make(chan Event, len(a.events)+1)}, nil
}

func (a *gatewayTestAgent) StartSessionWithOptions(_ context.Context, _ string, opts AgentSessionOptions) (AgentSession, error) {
	a.mu.Lock()
	a.startCount++
	a.sessionOptions = append(a.sessionOptions, opts)
	a.mu.Unlock()
	return &gatewayTestSession{agent: a, events: make(chan Event, len(a.events)+1)}, nil
}

func (a *gatewayTestAgent) StartPersistentSession(_ context.Context, _ string, opts AgentSessionOptions) (AgentSession, error) {
	a.mu.Lock()
	a.persistentStarts++
	a.sessionOptions = append(a.sessionOptions, opts)
	a.mu.Unlock()
	return &gatewayTestSession{agent: a, events: make(chan Event, len(a.events)+1)}, nil
}

func (a *gatewayTestAgent) StartIsolatedSession(_ context.Context, _ string, opts AgentSessionOptions) (AgentSession, error) {
	a.mu.Lock()
	a.isolatedStarts++
	a.sessionOptions = append(a.sessionOptions, opts)
	a.mu.Unlock()
	return &gatewayTestSession{agent: a, events: make(chan Event, len(a.events)+1)}, nil
}

func (a *gatewayTestAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}

func (a *gatewayTestAgent) Stop() error { return nil }

func (a *gatewayTestAgent) SetModel(model string) {
	a.mu.Lock()
	a.defaultModel = model
	a.mu.Unlock()
}

func (a *gatewayTestAgent) GetModel() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.defaultModel
}

func (a *gatewayTestAgent) AvailableModels(context.Context) []ModelOption {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]ModelOption(nil), a.models...)
}

func (a *gatewayTestAgent) SetReasoningEffort(string)  {}
func (a *gatewayTestAgent) GetReasoningEffort() string { return "" }
func (a *gatewayTestAgent) AvailableReasoningEfforts() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.reasoningEfforts...)
}

func (a *gatewayTestAgent) lastPrompt() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.prompts) == 0 {
		return ""
	}
	return a.prompts[len(a.prompts)-1]
}

func (a *gatewayTestAgent) lastSessionOptions() AgentSessionOptions {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.sessionOptions) == 0 {
		return AgentSessionOptions{}
	}
	return a.sessionOptions[len(a.sessionOptions)-1]
}

type gatewayTestSession struct {
	agent  *gatewayTestAgent
	events chan Event
	alive  bool
}

func (s *gatewayTestSession) Send(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.agent.mu.Lock()
	s.agent.prompts = append(s.agent.prompts, prompt)
	events := append([]Event(nil), s.agent.events...)
	s.agent.mu.Unlock()
	s.alive = true
	for _, event := range events {
		s.events <- event
	}
	return nil
}

func (s *gatewayTestSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *gatewayTestSession) Events() <-chan Event                             { return s.events }
func (s *gatewayTestSession) CurrentSessionID() string                         { return "gateway-session" }
func (s *gatewayTestSession) Alive() bool                                      { return s.alive }
func (s *gatewayTestSession) Close() error {
	s.alive = false
	s.agent.mu.Lock()
	s.agent.closeCount++
	s.agent.mu.Unlock()
	return nil
}

func (s *gatewayTestSession) ResetConversation(context.Context) error {
	s.agent.mu.Lock()
	defer s.agent.mu.Unlock()
	s.agent.resetCount++
	return s.agent.resetErr
}

func newGatewayTestServer(agent *gatewayTestAgent, token string) *OpenAIGateway {
	engine := NewEngine("demo", agent, nil, "", LangEnglish)
	gateway := NewOpenAIGateway("127.0.0.1:0", token, map[string]string{"codex-cli": "demo"}, 0)
	gateway.RegisterEngine("demo", engine)
	return gateway
}

func TestOpenAIGateway_ModelsRequireBearerToken(t *testing.T) {
	gateway := newGatewayTestServer(&gatewayTestAgent{}, "secret")

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"id":"codex-cli"`) {
		t.Fatalf("models response missing codex-cli: %s", rec.Body.String())
	}
}

func TestOpenAIGateway_TextOnlyFiltersUnsupportedAgentsAndPropagatesRequirement(t *testing.T) {
	textBase := &gatewayTestAgent{events: []Event{{Type: EventResult, Content: "text-only-ok", Done: true}}}
	sandboxedBase := &gatewayTestAgent{events: []Event{{Type: EventResult, Content: "sandboxed-text-ok", Done: true}}}
	unsupported := &gatewayTestAgent{}
	gateway := NewOpenAIGateway("127.0.0.1:0", "", map[string]string{
		"qoder-cli":       "text-project",
		"codex-cli":       "sandboxed-project",
		"unsupported-cli": "unsupported-project",
	}, 0)
	gateway.RegisterEngine("text-project", NewEngine("text-project", &gatewayTextOnlyTestAgent{gatewayTestAgent: textBase}, nil, "", LangEnglish))
	gateway.RegisterEngine("sandboxed-project", NewEngine("sandboxed-project", &gatewaySandboxedTextTestAgent{gatewayTestAgent: sandboxedBase}, nil, "", LangEnglish))
	gateway.RegisterEngine("unsupported-project", NewEngine("unsupported-project", unsupported, nil, "", LangEnglish))
	gateway.ConfigureTextOnly(true)

	modelsReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	modelsRec := httptest.NewRecorder()
	gateway.ServeHTTP(modelsRec, modelsReq)
	if modelsRec.Code != http.StatusOK {
		t.Fatalf("models status = %d; body=%s", modelsRec.Code, modelsRec.Body.String())
	}
	if strings.Contains(modelsRec.Body.String(), `"id":"unsupported-cli"`) || !strings.Contains(modelsRec.Body.String(), `"id":"qoder-cli"`) || !strings.Contains(modelsRec.Body.String(), `"id":"codex-cli"`) {
		t.Fatalf("strict model filtering failed: %s", modelsRec.Body.String())
	}
	if !strings.Contains(modelsRec.Body.String(), `"cc_capability_mode":"zero_tools"`) || !strings.Contains(modelsRec.Body.String(), `"cc_capability_mode":"sandboxed_text"`) {
		t.Fatalf("models response lacks concrete capability modes: %s", modelsRec.Body.String())
	}

	unsupportedReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"unsupported-cli","messages":[{"role":"user","content":"hello"}]}`))
	unsupportedRec := httptest.NewRecorder()
	gateway.ServeHTTP(unsupportedRec, unsupportedReq)
	if unsupportedRec.Code != http.StatusBadRequest || !strings.Contains(unsupportedRec.Body.String(), "text_only_unsupported") {
		t.Fatalf("unsupported status=%d body=%s", unsupportedRec.Code, unsupportedRec.Body.String())
	}
	if unsupported.startCount != 0 {
		t.Fatalf("unsupported agent start count = %d, want 0", unsupported.startCount)
	}

	textReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"qoder-cli","messages":[{"role":"user","content":"hello"}]}`))
	textRec := httptest.NewRecorder()
	gateway.ServeHTTP(textRec, textReq)
	if textRec.Code != http.StatusOK {
		t.Fatalf("text-only status=%d body=%s", textRec.Code, textRec.Body.String())
	}
	if got := textRec.Header().Get("X-CC-Capability-Mode"); got != AgentCapabilityZeroTools {
		t.Fatalf("capability header = %q, want %s", got, AgentCapabilityZeroTools)
	}
	if opts := textBase.lastSessionOptions(); !opts.TextOnly || opts.CapabilityMode != AgentCapabilityZeroTools {
		t.Fatalf("session options = %+v, want zero-tools text mode", opts)
	}

	sandboxedReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"codex-cli","messages":[{"role":"user","content":"hello"}]}`))
	sandboxedRec := httptest.NewRecorder()
	gateway.ServeHTTP(sandboxedRec, sandboxedReq)
	if sandboxedRec.Code != http.StatusOK {
		t.Fatalf("sandboxed-text status=%d body=%s", sandboxedRec.Code, sandboxedRec.Body.String())
	}
	if got := sandboxedRec.Header().Get("X-CC-Capability-Mode"); got != AgentCapabilitySandboxedText {
		t.Fatalf("capability header = %q, want %s", got, AgentCapabilitySandboxedText)
	}
	if opts := sandboxedBase.lastSessionOptions(); !opts.TextOnly || opts.CapabilityMode != AgentCapabilitySandboxedText {
		t.Fatalf("session options = %+v, want sandboxed-text mode", opts)
	}
}

func TestOpenAIGateway_AccessLogTracksPersistentSessionReuseWithoutSensitiveContent(t *testing.T) {
	agent := &gatewayTestAgent{
		defaultModel:     "test-model",
		reasoningEfforts: []string{"low"},
		events: []Event{
			{Type: EventText, Content: "sensitive-response"},
			{Type: EventResult, Done: true},
		},
	}
	gateway := newGatewayTestServer(agent, "sensitive-token")
	gateway.ConfigureAccessLogging(true)
	gateway.ConfigurePersistentSessions(true, time.Minute, 2)
	defer gateway.stopPersistentSessions()

	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(previousLogger)

	unauthorized := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	gateway.ServeHTTP(httptest.NewRecorder(), unauthorized)

	firstBody := `{"model":"codex-cli","reasoning_effort":"low","cc_session":true,"messages":[{"role":"user","content":"sensitive-prompt-one"}]}`
	firstRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(firstBody))
	firstRequest.Header.Set("Authorization", "Bearer sensitive-token")
	firstRecorder := httptest.NewRecorder()
	gateway.ServeHTTP(firstRecorder, firstRequest)
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("first status = %d; body=%s", firstRecorder.Code, firstRecorder.Body.String())
	}
	var firstResponse openAIChatCompletionResponse
	if err := json.Unmarshal(firstRecorder.Body.Bytes(), &firstResponse); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if firstResponse.CCSessionID == "" {
		t.Fatal("first response has no cc_session_id")
	}

	secondBody := fmt.Sprintf(`{"model":"codex-cli","reasoning_effort":"low","cc_session_id":%q,"messages":[{"role":"user","content":"sensitive-prompt-two"}]}`, firstResponse.CCSessionID)
	secondRequest := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(secondBody))
	secondRequest.Header.Set("Authorization", "Bearer sensitive-token")
	secondRecorder := httptest.NewRecorder()
	gateway.ServeHTTP(secondRecorder, secondRequest)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("second status = %d; body=%s", secondRecorder.Code, secondRecorder.Body.String())
	}

	logText := logs.String()
	for _, sensitive := range []string{"sensitive-token", "sensitive-prompt-one", "sensitive-prompt-two", "sensitive-response"} {
		if strings.Contains(logText, sensitive) {
			t.Fatalf("access log contains sensitive value %q: %s", sensitive, logText)
		}
	}

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logText), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log record: %v; line=%s", err, line)
		}
		if record["msg"] == "openai gateway access" {
			records = append(records, record)
		}
	}
	if len(records) != 3 {
		t.Fatalf("access record count = %d, want 3; logs=%s", len(records), logText)
	}
	if records[0]["status"] != float64(http.StatusUnauthorized) {
		t.Fatalf("unauthorized status = %v", records[0]["status"])
	}
	if records[1]["session_action"] != "create" || records[1]["session_id"] != firstResponse.CCSessionID {
		t.Fatalf("create record = %#v", records[1])
	}
	if records[2]["session_action"] != "reuse" || records[2]["session_id"] != firstResponse.CCSessionID {
		t.Fatalf("reuse record = %#v", records[2])
	}
	for _, record := range records[1:] {
		if record["model"] != "codex-cli" || record["project"] != "demo" || record["agent_model"] != "test-model" || record["reasoning_effort"] != "low" {
			t.Fatalf("missing routing fields: %#v", record)
		}
		if _, ok := record["duration_ms"]; !ok {
			t.Fatalf("duration_ms missing: %#v", record)
		}
	}
}

func TestOpenAIGateway_ChatCompletion(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{
		{Type: EventText, Content: "hello "},
		{Type: EventText, Content: "world"},
		{Type: EventResult, Done: true, InputTokens: 12, OutputTokens: 2},
	}}
	gateway := newGatewayTestServer(agent, "")
	body := `{
		"model":"codex-cli",
		"messages":[
			{"role":"developer","content":"Be concise."},
			{"role":"user","content":[{"type":"text","text":"Say hello"}]}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var response openAIChatCompletionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := response.Choices[0].Message.Content; got != "hello world" {
		t.Fatalf("content = %q, want hello world", got)
	}
	if response.Usage.PromptTokens != 12 || response.Usage.CompletionTokens != 2 || response.Usage.TotalTokens != 14 {
		t.Fatalf("unexpected usage: %+v", response.Usage)
	}
	prompt := agent.lastPrompt()
	if !strings.Contains(prompt, "[DEVELOPER]\nBe concise.") || !strings.Contains(prompt, "[USER]\nSay hello") {
		t.Fatalf("prompt did not preserve roles and text: %q", prompt)
	}
}

func TestOpenAIGateway_ExposesAndAppliesModelAndReasoningEffort(t *testing.T) {
	agent := &gatewayTestAgent{
		defaultModel:     "gpt-default",
		models:           []ModelOption{{Name: "gpt-fast"}, {Name: "gpt-smart"}},
		reasoningEfforts: []string{"low", "medium", "high", "xhigh", "max"},
		events:           []Event{{Type: EventResult, Content: "selected", Done: true}},
	}
	gateway := newGatewayTestServer(agent, "")

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("models status = %d; body=%s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{`"id":"codex-cli"`, `"id":"gpt-fast"`, `"id":"gpt-smart"`, `"reasoning_efforts":["low","medium","high","xhigh","max"]`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("models response missing %s: %s", want, rec.Body.String())
		}
	}

	body := `{"model":"gpt-smart","reasoning_effort":"xhigh","messages":[{"role":"user","content":"choose"}]}`
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec = httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("chat status = %d; body=%s", rec.Code, rec.Body.String())
	}
	if got := agent.lastSessionOptions(); got.Model != "gpt-smart" || got.ReasoningEffort != "xhigh" {
		t.Fatalf("session options = %+v", got)
	}
	if got := agent.GetModel(); got != "gpt-default" {
		t.Fatalf("shared model changed to %q", got)
	}
	if !strings.Contains(rec.Body.String(), `"reasoning_effort":"xhigh"`) {
		t.Fatalf("response did not echo reasoning effort: %s", rec.Body.String())
	}
}

func TestOpenAIGateway_MultipleProjectsPreserveUniqueFlatModelIDs(t *testing.T) {
	codexAgent := &gatewayTestAgent{
		models:           []ModelOption{{Name: "gpt-existing"}, {Name: "shared-model"}},
		reasoningEfforts: []string{"high"},
		events:           []Event{{Type: EventResult, Content: "codex", Done: true}},
	}
	qoderAgent := &gatewayTestAgent{
		models:           []ModelOption{{Name: "qoder-fast"}, {Name: "shared-model"}},
		reasoningEfforts: []string{"low"},
		events:           []Event{{Type: EventResult, Content: "qoder", Done: true}},
	}
	gateway := NewOpenAIGateway("127.0.0.1:0", "", map[string]string{
		"codex-cli": "codex-project",
		"qoder-cli": "qoder-project",
	}, 0)
	gateway.RegisterEngine("codex-project", NewEngine("codex-project", codexAgent, nil, "", LangEnglish))
	gateway.RegisterEngine("qoder-project", NewEngine("qoder-project", qoderAgent, nil, "", LangEnglish))

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("models status = %d; body=%s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{
		`"id":"gpt-existing"`, `"id":"codex-cli/gpt-existing"`,
		`"id":"qoder-fast"`, `"id":"qoder-cli/qoder-fast"`,
		`"id":"codex-cli/shared-model"`, `"id":"qoder-cli/shared-model"`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("models response missing %s: %s", want, rec.Body.String())
		}
	}
	if strings.Contains(rec.Body.String(), `"id":"shared-model"`) {
		t.Fatalf("ambiguous shared model was exposed without a namespace: %s", rec.Body.String())
	}

	body := `{"model":"gpt-existing","reasoning_effort":"high","messages":[{"role":"user","content":"keep compatibility"}]}`
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec = httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("flat model chat status = %d; body=%s", rec.Code, rec.Body.String())
	}
	if got := codexAgent.lastSessionOptions(); got.Model != "gpt-existing" || got.ReasoningEffort != "high" {
		t.Fatalf("flat model routed with options %+v", got)
	}

	body = `{"model":"shared-model","messages":[{"role":"user","content":"ambiguous"}]}`
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec = httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "ambiguous") {
		t.Fatalf("ambiguous model status = %d; body=%s", rec.Code, rec.Body.String())
	}
}

func TestOpenAIGateway_RejectsUnsupportedReasoningEffort(t *testing.T) {
	agent := &gatewayTestAgent{
		models:           []ModelOption{{Name: "gpt-smart"}},
		reasoningEfforts: []string{"low", "high"},
	}
	gateway := newGatewayTestServer(agent, "")
	body := `{"model":"gpt-smart","reasoning_effort":"max","messages":[{"role":"user","content":"choose"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"param":"reasoning_effort"`) {
		t.Fatalf("error missing reasoning_effort param: %s", rec.Body.String())
	}
}

func TestOpenAIGateway_StreamingChatCompletion(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{
		{Type: EventThinking, Content: "checking constraints"},
		{Type: EventText, Content: "first"},
		{Type: EventText, Content: " second"},
		{Type: EventResult, Done: true, InputTokens: 3, OutputTokens: 2},
	}, models: []ModelOption{{Name: "gpt-stream"}}, reasoningEfforts: []string{"high"}}
	gateway := newGatewayTestServer(agent, "")
	body := `{
		"model":"gpt-stream",
		"reasoning_effort":"high",
		"messages":[{"role":"user","content":"stream it"}],
		"stream":true,
		"stream_options":{"include_usage":true,"include_reasoning":true}
	}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}
	stream := rec.Body.String()
	for _, want := range []string{`"role":"assistant"`, `"reasoning_content":"checking constraints"`, `"content":"first"`, `"content":" second"`, `"reasoning_effort":"high"`, `"finish_reason":"stop"`, `"prompt_tokens":3`, "data: [DONE]"} {
		if !strings.Contains(stream, want) {
			t.Fatalf("stream missing %q: %s", want, stream)
		}
	}
	if got := agent.lastSessionOptions(); got.Model != "gpt-stream" || got.ReasoningEffort != "high" {
		t.Fatalf("stream session options = %+v", got)
	}
}

func TestOpenAIGateway_StreamingReasoningRequiresExplicitOptIn(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{
		{Type: EventThinking, Content: "private progress"},
		{Type: EventText, Content: "visible answer"},
		{Type: EventResult, Done: true},
	}, models: []ModelOption{{Name: "gpt-stream"}}}
	gateway := newGatewayTestServer(agent, "")
	body := `{"model":"gpt-stream","messages":[{"role":"user","content":"stream it"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	stream := rec.Body.String()
	if strings.Contains(stream, "reasoning_content") || strings.Contains(stream, "private progress") {
		t.Fatalf("reasoning leaked without include_reasoning opt-in: %s", stream)
	}
	if !strings.Contains(stream, `"content":"visible answer"`) {
		t.Fatalf("answer content missing: %s", stream)
	}
}

func TestOpenAIGateway_PersistentSessionReusesRuntimeAndSendsOnlyHistoryDelta(t *testing.T) {
	agent := &gatewayTestAgent{
		events:           []Event{{Type: EventResult, Content: "remembered", Done: true}},
		models:           []ModelOption{{Name: "gpt-fast"}},
		reasoningEfforts: []string{"low"},
	}
	gateway := newGatewayTestServer(agent, "")
	gateway.ConfigurePersistentSessions(true, time.Minute, 4)
	defer gateway.Stop()

	firstBody := `{"model":"gpt-fast","reasoning_effort":"low","cc_session":true,"messages":[{"role":"user","content":"remember alpha"}]}`
	firstReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(firstBody))
	firstRec := httptest.NewRecorder()
	gateway.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d; body=%s", firstRec.Code, firstRec.Body.String())
	}
	var first openAIChatCompletionResponse
	if err := json.Unmarshal(firstRec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if first.CCSessionID == "" || firstRec.Header().Get("X-CC-Session-ID") != first.CCSessionID {
		t.Fatalf("missing persistent session id: header=%q response=%q", firstRec.Header().Get("X-CC-Session-ID"), first.CCSessionID)
	}

	secondBody := fmt.Sprintf(`{"model":"gpt-fast","reasoning_effort":"low","cc_session_id":%q,"messages":[{"role":"user","content":"remember alpha"},{"role":"assistant","content":"remembered"},{"role":"user","content":"what next"}]}`, first.CCSessionID)
	secondReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(secondBody))
	secondRec := httptest.NewRecorder()
	gateway.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d; body=%s", secondRec.Code, secondRec.Body.String())
	}

	agent.mu.Lock()
	persistentStarts := agent.persistentStarts
	prompts := append([]string(nil), agent.prompts...)
	closeCount := agent.closeCount
	agent.mu.Unlock()
	if persistentStarts != 1 {
		t.Fatalf("persistent starts = %d, want 1", persistentStarts)
	}
	if closeCount != 0 {
		t.Fatalf("persistent runtime closed before gateway shutdown: %d", closeCount)
	}
	if len(prompts) != 2 || !strings.Contains(prompts[1], "[USER]\nwhat next") || strings.Contains(prompts[1], "remember alpha") {
		t.Fatalf("second prompt was not reduced to history delta: %#v", prompts)
	}
}

func TestOpenAIGateway_WarmResetReusesRuntimeAndClearsEveryRequest(t *testing.T) {
	agent := &gatewayTestAgent{
		events:           []Event{{Type: EventResult, Content: "isolated", Done: true}},
		models:           []ModelOption{{Name: "gpt-fast"}},
		reasoningEfforts: []string{"low"},
	}
	gateway := newGatewayTestServer(agent, "")
	gateway.ConfigurePersistentSessions(true, time.Minute, 4)
	defer gateway.Stop()

	firstBody := `{"model":"gpt-fast","reasoning_effort":"low","cc_session":true,"cc_session_mode":"warm_reset","messages":[{"role":"user","content":"task one"}]}`
	firstReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(firstBody))
	firstRec := httptest.NewRecorder()
	gateway.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusOK {
		t.Fatalf("first status = %d body=%s", firstRec.Code, firstRec.Body.String())
	}
	var first openAIChatCompletionResponse
	if err := json.Unmarshal(firstRec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if first.CCSessionID == "" || first.CCSessionMode != openAISessionModeWarmReset || first.CCContextReset != "ok" {
		t.Fatalf("first warm-reset metadata = %+v", first)
	}
	if got := firstRec.Header().Get("X-CC-Context-Reset"); got != "ok" {
		t.Fatalf("X-CC-Context-Reset = %q, want ok", got)
	}

	secondBody := fmt.Sprintf(`{"model":"gpt-fast","reasoning_effort":"low","cc_session_id":%q,"cc_session_mode":"warm_reset","messages":[{"role":"user","content":"task two"}]}`, first.CCSessionID)
	secondReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(secondBody))
	secondRec := httptest.NewRecorder()
	gateway.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second status = %d body=%s", secondRec.Code, secondRec.Body.String())
	}

	agent.mu.Lock()
	isolatedStarts := agent.isolatedStarts
	resetCount := agent.resetCount
	prompts := append([]string(nil), agent.prompts...)
	agent.mu.Unlock()
	if isolatedStarts != 1 || resetCount != 2 {
		t.Fatalf("isolated starts=%d resets=%d, want 1 and 2", isolatedStarts, resetCount)
	}
	if len(prompts) != 2 || !strings.Contains(prompts[0], "task one") || !strings.Contains(prompts[1], "task two") || strings.Contains(prompts[1], "task one") {
		t.Fatalf("warm-reset prompts were not isolated: %#v", prompts)
	}
}

func TestOpenAIGateway_WarmResetRejectsConversationHistory(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{{Type: EventResult, Content: "unused", Done: true}}}
	gateway := newGatewayTestServer(agent, "")
	gateway.ConfigurePersistentSessions(true, time.Minute, 4)
	defer gateway.Stop()

	body := `{"model":"codex-cli","cc_session_mode":"warm_reset","messages":[{"role":"system","content":"rules"},{"role":"user","content":"task"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "warm_reset_requires_single_user_message") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestOpenAIGateway_WarmResetFailureDiscardsRuntimeWithoutLosingResult(t *testing.T) {
	agent := &gatewayTestAgent{
		events:   []Event{{Type: EventResult, Content: "completed", Done: true}},
		resetErr: fmt.Errorf("reset unavailable"),
	}
	gateway := newGatewayTestServer(agent, "")
	gateway.ConfigurePersistentSessions(true, time.Minute, 4)
	defer gateway.Stop()

	body := `{"model":"codex-cli","cc_session_mode":"warm_reset","messages":[{"role":"user","content":"task"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response openAIChatCompletionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.CCSessionID != "" || response.CCContextReset != "failed" || response.Choices[0].Message.Content != "completed" {
		t.Fatalf("unexpected reset-failure response: %+v", response)
	}
	if got := rec.Header().Get("X-CC-Session-ID"); got != "" {
		t.Fatalf("reset-failure response exposed a non-reusable session id: %q", got)
	}
	agent.mu.Lock()
	closeCount := agent.closeCount
	agent.mu.Unlock()
	if closeCount != 1 {
		t.Fatalf("close count=%d, want 1", closeCount)
	}
}

func TestOpenAIGateway_PersistentSessionRejectsMismatchedHistoryWithoutClosingRuntime(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{{Type: EventResult, Content: "ok", Done: true}}}
	gateway := newGatewayTestServer(agent, "")
	gateway.ConfigurePersistentSessions(true, time.Minute, 4)
	defer gateway.Stop()

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"codex-cli","cc_session":true,"messages":[{"role":"user","content":"one"}]}`))
	firstRec := httptest.NewRecorder()
	gateway.ServeHTTP(firstRec, firstReq)
	var first openAIChatCompletionResponse
	if err := json.Unmarshal(firstRec.Body.Bytes(), &first); err != nil || first.CCSessionID == "" {
		t.Fatalf("create persistent session: status=%d err=%v body=%s", firstRec.Code, err, firstRec.Body.String())
	}

	mismatchBody := fmt.Sprintf(`{"model":"codex-cli","cc_session_id":%q,"messages":[{"role":"user","content":"different"},{"role":"user","content":"history"}]}`, first.CCSessionID)
	mismatchReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(mismatchBody))
	mismatchRec := httptest.NewRecorder()
	gateway.ServeHTTP(mismatchRec, mismatchReq)
	if mismatchRec.Code != http.StatusConflict || !strings.Contains(mismatchRec.Body.String(), "session_history_mismatch") {
		t.Fatalf("mismatch status=%d body=%s", mismatchRec.Code, mismatchRec.Body.String())
	}

	incrementalBody := fmt.Sprintf(`{"model":"codex-cli","cc_session_id":%q,"messages":[{"role":"user","content":"two"}]}`, first.CCSessionID)
	incrementalReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(incrementalBody))
	incrementalRec := httptest.NewRecorder()
	gateway.ServeHTTP(incrementalRec, incrementalReq)
	if incrementalRec.Code != http.StatusOK {
		t.Fatalf("session did not survive mismatch: status=%d body=%s", incrementalRec.Code, incrementalRec.Body.String())
	}
}

func TestOpenAIGateway_PersistentSessionExpiresAndClosesRuntime(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{{Type: EventResult, Content: "ok", Done: true}}}
	gateway := newGatewayTestServer(agent, "")
	gateway.ConfigurePersistentSessions(true, time.Millisecond, 4)
	defer gateway.Stop()

	firstReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"codex-cli","cc_session":true,"messages":[{"role":"user","content":"one"}]}`))
	firstRec := httptest.NewRecorder()
	gateway.ServeHTTP(firstRec, firstReq)
	var first openAIChatCompletionResponse
	if err := json.Unmarshal(firstRec.Body.Bytes(), &first); err != nil || first.CCSessionID == "" {
		t.Fatalf("create persistent session: status=%d err=%v body=%s", firstRec.Code, err, firstRec.Body.String())
	}
	time.Sleep(10 * time.Millisecond)

	secondBody := fmt.Sprintf(`{"model":"codex-cli","cc_session_id":%q,"messages":[{"role":"user","content":"two"}]}`, first.CCSessionID)
	secondReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(secondBody))
	secondRec := httptest.NewRecorder()
	gateway.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusNotFound || !strings.Contains(secondRec.Body.String(), "session_not_found") {
		t.Fatalf("expired status=%d body=%s", secondRec.Code, secondRec.Body.String())
	}

	agent.mu.Lock()
	closeCount := agent.closeCount
	agent.mu.Unlock()
	if closeCount != 1 {
		t.Fatalf("expired runtime close count = %d, want 1", closeCount)
	}
}

func TestOpenAIGateway_PersistentSessionsRequireConfiguration(t *testing.T) {
	agent := &gatewayTestAgent{events: []Event{{Type: EventResult, Content: "ok", Done: true}}}
	gateway := newGatewayTestServer(agent, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"codex-cli","cc_session":true,"messages":[{"role":"user","content":"one"}]}`))
	rec := httptest.NewRecorder()
	gateway.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "persistent_sessions_disabled") {
		t.Fatalf("disabled status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestOpenAIGateway_RejectsUnsupportedToolsAndImages(t *testing.T) {
	gateway := newGatewayTestServer(&gatewayTestAgent{}, "")
	tests := []string{
		`{"model":"codex-cli","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function"}]}`,
		`{"model":"codex-cli","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}]}`,
		`{"model":"codex-cli","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_schema"}}`,
	}
	for _, body := range tests {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		rec := httptest.NewRecorder()
		gateway.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
		}
	}
}

func TestOpenAIGateway_RequiresTokenForNonLoopbackListen(t *testing.T) {
	gateway := NewOpenAIGateway("0.0.0.0:0", "", nil, 0)
	if err := gateway.Start(); err == nil {
		gateway.Stop()
		t.Fatal("Start succeeded without a token on a non-loopback address")
	}
}
