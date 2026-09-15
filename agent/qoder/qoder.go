package qoder

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func init() {
	core.RegisterAgent("qoder", New)
}

// Agent drives Qoder CLI using `qodercli -p <prompt> -f stream-json`.
type Agent struct {
	workDir          string
	cmd              string   // CLI binary name (default: "qodercli")
	cliExtraArgs     []string // extra args from cmd after the binary name
	configEnv        []string // env vars from [projects.agent.options.env]
	model            string
	reasoningEffort  string
	mode             string // "default" | "yolo"
	sessionEnv       []string
	availableModels  []core.ModelOption
	modelsRefreshing bool
	modelCachePath   string
	mu               sync.Mutex
}

type qoderPersistentModelCache struct {
	Models    []core.ModelOption `json:"models"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type qoderModelDiscoverySnapshot struct {
	cmd       string
	extraArgs []string
	workDir   string
	extraEnv  []string
	cachePath string
}

func New(opts map[string]any) (core.Agent, error) {
	workDir, _ := opts["work_dir"].(string)
	if workDir == "" {
		workDir = "."
	}
	model, _ := opts["model"].(string)
	reasoningEffort, _ := opts["reasoning_effort"].(string)
	mode, _ := opts["mode"].(string)
	mode = normalizeMode(mode)

	cmd, extraArgs := core.ParseCmdOpts(opts, "qodercli")
	ccDataDir, _ := opts["cc_data_dir"].(string)
	ccProject, _ := opts["cc_project"].(string)
	modelCachePath := qoderProjectModelCachePath(ccDataDir, ccProject)
	availableModels, err := loadQoderPersistentModelCache(modelCachePath)
	if err != nil {
		slog.Warn("qoder: load persistent model cache failed", "path", modelCachePath, "error", err)
	}
	if _, err := exec.LookPath(cmd); err != nil {
		return nil, fmt.Errorf("qoder: %q not found in PATH, install with: curl -fsSL https://qoder.com/install | bash", cmd)
	}

	return &Agent{
		workDir:         workDir,
		cmd:             cmd,
		cliExtraArgs:    extraArgs,
		configEnv:       core.ParseConfigEnv(opts),
		model:           model,
		reasoningEffort: normalizeReasoningEffort(reasoningEffort),
		mode:            mode,
		availableModels: availableModels,
		modelCachePath:  modelCachePath,
	}, nil
}

func qoderProjectModelCachePath(dataDir, project string) string {
	if strings.TrimSpace(dataDir) == "" || strings.TrimSpace(project) == "" {
		return ""
	}
	return filepath.Join(dataDir, "projects", fmt.Sprintf("%x.qoder-models.json", sha256.Sum256([]byte(project))))
}

func loadQoderPersistentModelCache(path string) ([]core.ModelOption, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cache qoderPersistentModelCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}
	return normalizeQoderModelOptions(cache.Models), nil
}

func normalizeQoderModelOptions(models []core.ModelOption) []core.ModelOption {
	seen := make(map[string]struct{}, len(models))
	normalized := make([]core.ModelOption, 0, len(models))
	for _, model := range models {
		model.Name = strings.TrimSpace(model.Name)
		if model.Name == "" {
			continue
		}
		if _, duplicate := seen[model.Name]; duplicate {
			continue
		}
		seen[model.Name] = struct{}{}
		normalized = append(normalized, model)
	}
	return normalized
}

func normalizeMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "yolo", "bypass", "dangerously-skip-permissions":
		return "yolo"
	default:
		return "default"
	}
}

func (a *Agent) Name() string           { return "qoder" }
func (a *Agent) CLIBinaryName() string  { return a.cmd }
func (a *Agent) CLIDisplayName() string { return "Qoder" }

func (a *Agent) SetWorkDir(dir string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.workDir = dir
	slog.Info("qoder: work_dir changed", "work_dir", dir)
}

func (a *Agent) GetWorkDir() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.workDir
}

func (a *Agent) SetModel(model string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.model = model
	slog.Info("qoder: model changed", "model", model)
}

func (a *Agent) GetModel() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.model
}

func (a *Agent) AvailableModels(_ context.Context) []core.ModelOption {
	a.mu.Lock()
	if len(a.availableModels) > 0 {
		models := append([]core.ModelOption(nil), a.availableModels...)
		a.mu.Unlock()
		return models
	}
	if a.modelsRefreshing {
		a.mu.Unlock()
		return qoderFallbackModels()
	}
	a.modelsRefreshing = true
	snapshot := a.modelDiscoverySnapshotLocked()
	a.mu.Unlock()
	go a.refreshAvailableModels(snapshot)
	return qoderFallbackModels()
}

func (a *Agent) modelDiscoverySnapshotLocked() qoderModelDiscoverySnapshot {
	extraEnv := append([]string(nil), a.configEnv...)
	extraEnv = append(extraEnv, a.sessionEnv...)
	return qoderModelDiscoverySnapshot{
		cmd:       a.cmd,
		extraArgs: append([]string(nil), a.cliExtraArgs...),
		workDir:   a.workDir,
		extraEnv:  extraEnv,
		cachePath: a.modelCachePath,
	}
}

func (a *Agent) StartInitialModelRefresh() {
	a.mu.Lock()
	if a.modelsRefreshing {
		a.mu.Unlock()
		return
	}
	a.modelsRefreshing = true
	snapshot := a.modelDiscoverySnapshotLocked()
	a.mu.Unlock()
	go a.refreshAvailableModels(snapshot)
}

func (a *Agent) refreshAvailableModels(snapshot qoderModelDiscoverySnapshot) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := append(snapshot.extraArgs, "--list-models")
	cmd := exec.CommandContext(ctx, snapshot.cmd, args...)
	cmd.Dir = snapshot.workDir
	if len(snapshot.extraEnv) > 0 {
		cmd.Env = core.MergeEnv(os.Environ(), snapshot.extraEnv)
	}
	output, err := cmd.Output()
	models := normalizeQoderModelOptions(parseQoderModelsOutput(string(output)))

	if err == nil && len(models) > 0 {
		if cacheErr := storeQoderPersistentModelCache(snapshot.cachePath, models); cacheErr != nil {
			slog.Warn("qoder: update persistent model cache failed", "path", snapshot.cachePath, "error", cacheErr)
		}
	}
	a.mu.Lock()
	a.modelsRefreshing = false
	if err == nil && len(models) > 0 {
		a.availableModels = append([]core.ModelOption(nil), models...)
	}
	a.mu.Unlock()

	if err != nil {
		slog.Warn("qoder: failed to refresh model list; keeping built-in fallback", "error", err)
	} else if len(models) == 0 {
		slog.Warn("qoder: model list was empty; keeping built-in fallback")
	} else {
		slog.Info("qoder: refreshed available models", "count", len(models))
	}
}

func storeQoderPersistentModelCache(path string, models []core.ModelOption) error {
	if path == "" || len(models) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(qoderPersistentModelCache{Models: models, UpdatedAt: time.Now()})
	if err != nil {
		return err
	}
	return core.AtomicWriteFile(path, data, 0o644)
}

func parseQoderModelsOutput(output string) []core.ModelOption {
	var models []core.ModelOption
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.EqualFold(line, "MODEL") {
			continue
		}
		option := core.ModelOption{Name: line}
		if i := strings.LastIndex(line, " ("); i > 0 && strings.HasSuffix(line, ")") {
			if id := strings.TrimSpace(line[i+2 : len(line)-1]); id != "" {
				option.Name = id
				option.Desc = strings.TrimSpace(line[:i])
			}
		}
		if option.Desc == "" {
			switch normalized := strings.ToLower(option.Name); normalized {
			case "auto", "ultimate", "performance", "efficient", "lite":
				option.Name = normalized
			}
		}
		models = append(models, option)
	}
	return models
}

func qoderFallbackModels() []core.ModelOption {
	return []core.ModelOption{
		{Name: "auto", Desc: "Auto (recommended)"},
		{Name: "ultimate", Desc: "Ultimate (most capable)"},
		{Name: "performance", Desc: "Performance (balanced)"},
		{Name: "efficient", Desc: "Efficient (fast)"},
		{Name: "lite", Desc: "Lite (lightweight)"},
	}
}

func normalizeReasoningEffort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "auto", "none", "low", "medium", "high", "xhigh", "max", "ultracode":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func (a *Agent) SetReasoningEffort(effort string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reasoningEffort = normalizeReasoningEffort(effort)
	slog.Info("qoder: reasoning effort changed", "reasoning_effort", a.reasoningEffort)
}

func (a *Agent) GetReasoningEffort() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reasoningEffort
}

func (a *Agent) AvailableReasoningEfforts() []string {
	return []string{"auto", "none", "low", "medium", "high", "xhigh", "max", "ultracode"}
}

func (a *Agent) SetSessionEnv(env []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessionEnv = env
}

func (a *Agent) StartSession(ctx context.Context, sessionID string) (core.AgentSession, error) {
	return a.startSession(ctx, sessionID, core.AgentSessionOptions{})
}

// SupportsTextOnlySessions reports that Qoder can enforce a zero-tool runtime
// with its documented --tools "" process option.
func (a *Agent) SupportsTextOnlySessions() bool { return true }

// StartSessionWithOptions applies model and reasoning overrides only to the
// newly created session, leaving the shared Qoder defaults unchanged.
func (a *Agent) StartSessionWithOptions(ctx context.Context, sessionID string, opts core.AgentSessionOptions) (core.AgentSession, error) {
	return a.startSession(ctx, sessionID, opts)
}

// StartPersistentSession starts Qoder in its bidirectional SDK transport mode
// so one qodercli process can serve multiple turns without a cold start between
// them. It is used by explicitly persistent API sessions only.
func (a *Agent) StartPersistentSession(ctx context.Context, sessionID string, opts core.AgentSessionOptions) (core.AgentSession, error) {
	return a.startPersistentSession(ctx, sessionID, opts, false)
}

// StartIsolatedSession starts the same bidirectional SDK transport and enables
// a conversation-only rewind after every completed turn.
func (a *Agent) StartIsolatedSession(ctx context.Context, sessionID string, opts core.AgentSessionOptions) (core.AgentSession, error) {
	return a.startPersistentSession(ctx, sessionID, opts, true)
}

func (a *Agent) startPersistentSession(ctx context.Context, sessionID string, opts core.AgentSessionOptions, isolated bool) (core.AgentSession, error) {
	a.mu.Lock()
	mode := a.mode
	model := a.model
	reasoningEffort := a.reasoningEffort
	cmd := a.cmd
	extraArgs := append([]string{}, a.cliExtraArgs...)
	workDir := a.workDir
	extraEnv := append([]string(nil), a.configEnv...)
	extraEnv = append(extraEnv, a.sessionEnv...)
	a.mu.Unlock()

	if requestedModel := strings.TrimSpace(opts.Model); requestedModel != "" {
		model = requestedModel
	}
	if requestedEffort := strings.TrimSpace(opts.ReasoningEffort); requestedEffort != "" {
		reasoningEffort = normalizeReasoningEffort(requestedEffort)
		if reasoningEffort == "" {
			return nil, fmt.Errorf("qoder: unsupported reasoning effort %q", requestedEffort)
		}
	}

	zeroTools := opts.TextOnly && (opts.CapabilityMode == "" || opts.CapabilityMode == core.AgentCapabilityZeroTools)
	return newQoderPersistentSession(ctx, cmd, extraArgs, workDir, model, reasoningEffort, mode, sessionID, extraEnv, isolated, zeroTools)
}

func (a *Agent) startSession(ctx context.Context, sessionID string, opts core.AgentSessionOptions) (core.AgentSession, error) {
	a.mu.Lock()
	mode := a.mode
	model := a.model
	reasoningEffort := a.reasoningEffort
	cmd := a.cmd
	extraArgs := append([]string{}, a.cliExtraArgs...)
	workDir := a.workDir
	extraEnv := append([]string(nil), a.configEnv...)
	extraEnv = append(extraEnv, a.sessionEnv...)
	a.mu.Unlock()

	if requestedModel := strings.TrimSpace(opts.Model); requestedModel != "" {
		model = requestedModel
	}
	if requestedEffort := strings.TrimSpace(opts.ReasoningEffort); requestedEffort != "" {
		reasoningEffort = normalizeReasoningEffort(requestedEffort)
		if reasoningEffort == "" {
			return nil, fmt.Errorf("qoder: unsupported reasoning effort %q", requestedEffort)
		}
	}

	zeroTools := opts.TextOnly && (opts.CapabilityMode == "" || opts.CapabilityMode == core.AgentCapabilityZeroTools)
	return newQoderSession(ctx, cmd, extraArgs, workDir, model, reasoningEffort, mode, sessionID, extraEnv, zeroTools)
}

func (a *Agent) ListSessions(_ context.Context) ([]core.AgentSessionInfo, error) {
	return nil, nil
}

func (a *Agent) Stop() error { return nil }

// ── ModeSwitcher ─────────────────────────────────────────────

func (a *Agent) SetMode(mode string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mode = normalizeMode(mode)
	slog.Info("qoder: mode changed", "mode", a.mode)
}

func (a *Agent) GetMode() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mode
}

func (a *Agent) PermissionModes() []core.PermissionModeInfo {
	return []core.PermissionModeInfo{
		{Key: "default", Name: "Default", NameZh: "默认", Desc: "Standard permissions", DescZh: "标准权限模式"},
		{Key: "yolo", Name: "YOLO", NameZh: "全自动", Desc: "Skip all permission checks", DescZh: "跳过所有权限检查"},
	}
}

// ── SkillProvider ────────────────────────────────────────────

func (a *Agent) SkillDirs() []string {
	workDir := a.GetWorkDir()
	absDir, err := filepath.Abs(workDir)
	if err != nil {
		absDir = workDir
	}
	dirs := []string{filepath.Join(absDir, ".claude", "skills")}
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".claude", "skills"))
	}
	return dirs
}

// ── ContextCompressor ────────────────────────────────────────

func (a *Agent) CompressCommand() string { return "/compact" }

// ── MemoryFileProvider ───────────────────────────────────────

func (a *Agent) ProjectMemoryFile() string {
	workDir := a.GetWorkDir()
	absDir, err := filepath.Abs(workDir)
	if err != nil {
		absDir = workDir
	}
	return filepath.Join(absDir, "AGENTS.md")
}

func (a *Agent) GlobalMemoryFile() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(homeDir, ".qoder", "AGENTS.md")
}
