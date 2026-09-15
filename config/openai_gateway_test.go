package config

import (
	"testing"

	"github.com/BurntSushi/toml"
)

func TestOpenAIGatewayConfigParsing(t *testing.T) {
	input := `
[openai_gateway]
enabled = true
listen = "127.0.0.1:9840"
token = "local-secret"
timeout_secs = 300
access_log = true
text_only = true
persistent_sessions = true
session_idle_timeout_secs = 900
max_persistent_sessions = 8

[openai_gateway.models]
codex-cli = "demo-project"
`
	var cfg Config
	if _, err := toml.Decode(input, &cfg); err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if cfg.OpenAIGateway.Enabled == nil || !*cfg.OpenAIGateway.Enabled {
		t.Fatal("openai_gateway.enabled was not parsed")
	}
	if cfg.OpenAIGateway.Listen != "127.0.0.1:9840" {
		t.Fatalf("listen = %q", cfg.OpenAIGateway.Listen)
	}
	if cfg.OpenAIGateway.Token != "local-secret" {
		t.Fatalf("token = %q", cfg.OpenAIGateway.Token)
	}
	if cfg.OpenAIGateway.TimeoutSecs != 300 {
		t.Fatalf("timeout_secs = %d", cfg.OpenAIGateway.TimeoutSecs)
	}
	if cfg.OpenAIGateway.AccessLog == nil || !*cfg.OpenAIGateway.AccessLog {
		t.Fatal("access_log was not parsed")
	}
	if cfg.OpenAIGateway.TextOnly == nil || !*cfg.OpenAIGateway.TextOnly {
		t.Fatal("text_only was not parsed")
	}
	if cfg.OpenAIGateway.PersistentSessions == nil || !*cfg.OpenAIGateway.PersistentSessions {
		t.Fatal("persistent_sessions was not parsed")
	}
	if cfg.OpenAIGateway.SessionIdleTimeoutSecs != 900 {
		t.Fatalf("session_idle_timeout_secs = %d", cfg.OpenAIGateway.SessionIdleTimeoutSecs)
	}
	if cfg.OpenAIGateway.MaxPersistentSessions != 8 {
		t.Fatalf("max_persistent_sessions = %d", cfg.OpenAIGateway.MaxPersistentSessions)
	}
	if got := cfg.OpenAIGateway.Models["codex-cli"]; got != "demo-project" {
		t.Fatalf("models[codex-cli] = %q", got)
	}
}

func TestOpenAIGatewayAllowsMappedPlatformlessProject(t *testing.T) {
	enabled := true
	cfg := Config{
		OpenAIGateway: OpenAIGatewayConfig{
			Enabled: &enabled,
			Models:  map[string]string{"qoder-cli": "qoder-project"},
		},
		Projects: []ProjectConfig{{
			Name:  "qoder-project",
			Agent: AgentConfig{Type: "qoder"},
		}},
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate mapped platformless project: %v", err)
	}
}

func TestOpenAIGatewayDoesNotAllowUnmappedPlatformlessProject(t *testing.T) {
	enabled := true
	cfg := Config{
		OpenAIGateway: OpenAIGatewayConfig{
			Enabled: &enabled,
			Models:  map[string]string{"codex-cli": "other-project"},
		},
		Projects: []ProjectConfig{{
			Name:  "qoder-project",
			Agent: AgentConfig{Type: "qoder"},
		}},
	}
	if err := cfg.validate(); err == nil {
		t.Fatal("validate unexpectedly allowed an unmapped platformless project")
	}
}

func TestOpenAIGatewayRejectsNegativeSessionLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*OpenAIGatewayConfig)
	}{
		{name: "request timeout", mutate: func(g *OpenAIGatewayConfig) { g.TimeoutSecs = -1 }},
		{name: "idle timeout", mutate: func(g *OpenAIGatewayConfig) { g.SessionIdleTimeoutSecs = -1 }},
		{name: "capacity", mutate: func(g *OpenAIGatewayConfig) { g.MaxPersistentSessions = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Projects: []ProjectConfig{{Name: "demo", Agent: AgentConfig{Type: "qoder"}}}}
			tt.mutate(&cfg.OpenAIGateway)
			if err := cfg.validatePermissive(); err == nil {
				t.Fatal("validate unexpectedly accepted a negative OpenAI gateway limit")
			}
		})
	}
}
