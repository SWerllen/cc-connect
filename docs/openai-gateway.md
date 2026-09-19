# OpenAI-compatible gateway

cc-connect can expose a project through a small, text-only subset of the OpenAI Chat Completions API. This is useful for clients that accept a custom OpenAI base URL but should execute requests through a local coding agent.

The gateway implements:

- `GET /v1/models`
- `POST /v1/chat/completions`
- non-streaming responses
- `stream: true` Server-Sent Events (SSE)
- Bearer token authentication
- optional structured access logs without credentials or message bodies
- optional isolated text execution (`text_only = true`)
- per-request Codex model selection
- per-request `reasoning_effort` selection
- opt-in persistent Codex and Qoder runtime sessions
- string content and arrays of text content parts

The request and response shapes follow the [OpenAI Chat Completions API](https://developers.openai.com/api/reference/cli/resources/chat/subresources/completions).

## Configuration

```toml
[openai_gateway]
enabled = true
listen = "127.0.0.1:9840"
token = "replace-with-a-random-secret"
timeout_secs = 600
access_log = true
text_only = true
persistent_sessions = true
session_idle_timeout_secs = 900
max_persistent_sessions = 16

[openai_gateway.models]
qoder-cli = "my-qoder-project"
codex-cli = "my-codex-project"
```

When `text_only = true`, the gateway fails closed. It exposes only agents that
provide a concrete text-session isolation mode. Qoder enforces `zero_tools`
with its documented `--tools ""` option. Codex exposes `sandboxed_text`: it
retains internal built-in schemas, but each gateway session runs in a dedicated
empty directory with a read-only sandbox, `approvalPolicy=never`, an ephemeral
thread, and no dynamic tools. Each model object and completion reports its
specific `cc_capability_mode`; the completion response header reports the same
value. Agents that provide neither guarantee remain hidden and fail with
`400 text_only_unsupported` when explicitly requested.

This is an ingress-specific setting. Feishu sessions and direct CLI use keep
their existing agent mode and tool access, including `full-auto`/yolo settings.
For a Chinese integration and verification checklist, see
[隔离文本模式接入说明](openai-gateway-text-only.zh-CN.md).

`models` maps a public project alias used by OpenAI clients to a cc-connect project name. If the map is omitted, every configured project is exposed using its project name.

`GET /v1/models` includes each project alias plus the native models reported by its agent. Native model IDs such as `gpt-5.4` remain available without a prefix when they identify exactly one project, preserving compatibility as more projects are added. Namespaced IDs in the form `<project-alias>/<agent-model>` are also exposed, and are required when two projects report the same native model ID. Each model object also includes the cc-connect project, native agent model, and supported `reasoning_efforts` as extension fields.

The gateway refuses to start without a token when `listen` is not a loopback address. Keep it on `127.0.0.1` unless you have a trusted reverse proxy, TLS, and network-level access controls.

When `access_log = true`, cc-connect writes one structured `openai gateway
access` record per HTTP request to its normal log. Records include the method,
path, status, duration, response size, client address, model, project, backing
agent model, reasoning effort, stream flag, request ID, and `session_action`
(`stateless`, `create`, or `reuse`) when applicable. Persistent requests also
include their ephemeral `session_id`; isolated requests also include
`capability_mode=zero_tools` or `capability_mode=sandboxed_text`. Authorization headers, request bodies,
prompts, and response content are never logged.

## Requests

List models:

```bash
curl http://127.0.0.1:9840/v1/models \
  -H "Authorization: Bearer replace-with-a-random-secret"
```

Create a completion:

```bash
curl http://127.0.0.1:9840/v1/chat/completions \
  -H "Authorization: Bearer replace-with-a-random-secret" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qoder-cli",
    "reasoning_effort": "high",
    "messages": [
      {"role": "developer", "content": "Be concise."},
      {"role": "user", "content": "Summarize this repository."}
    ]
  }'
```

With the OpenAI Python SDK:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:9840/v1",
    api_key="replace-with-a-random-secret",
)

response = client.chat.completions.create(
    model="qoder-cli",
    reasoning_effort="high",
    messages=[{"role": "user", "content": "Explain the current project."}],
)
print(response.choices[0].message.content)
```

For streaming, set `stream=True` in an SDK or `"stream": true` in JSON. `stream_options.include_usage` is supported. Clients that want agent-provided reasoning summaries or progress may also opt in with `stream_options.include_reasoning: true`; those chunks use the OpenAI-compatible extension `delta.reasoning_content`. Reasoning is never mixed into `delta.content`, and provider-redacted reasoning is never forwarded.

For a production-oriented Chinese integration example, including incremental
JSON assembly, cancellation, session reset, and timeout guidance, see
[导演台 SSE 与推理进度接入说明](openai-gateway-director-streaming.zh-CN.md).

### Persistent sessions

For a Chinese developer integration guide with Node.js examples and an
operational troubleshooting checklist, see
[OpenAI 网关 Session 复用接入说明](openai-gateway-session-reuse.zh-CN.md).

For independent jobs that should reuse only the runtime process while clearing
agent context after every response, use
[`cc_session_mode: "warm_reset"`](openai-gateway-warm-reset.zh-CN.md).

Persistent sessions are an opt-in cc-connect extension. They keep the backing
agent process alive between turns while leaving
ordinary OpenAI-compatible requests stateless.

Create a session by adding `"cc_session": true` to the first request. The
response returns `cc_session_id` in both the JSON body and the
`X-CC-Session-ID` header:

```json
{
  "model": "qoder-cli/Qwen3.8-Flash",
  "reasoning_effort": "low",
  "cc_session": true,
  "messages": [{"role": "user", "content": "Remember the number 42."}]
}
```

Send the returned ID on later turns:

```json
{
  "model": "qoder-cli/Qwen3.8-Flash",
  "reasoning_effort": "low",
  "cc_session_id": "ccs_returned_by_the_first_request",
  "messages": [{"role": "user", "content": "What number did I give you?"}]
}
```

Clients may instead send their complete OpenAI `messages` history. The gateway
verifies that it extends the stored history and forwards only the new suffix.
This prevents duplicate context. A history mismatch returns HTTP 409 without
destroying the healthy session. `X-CC-Session-ID: new` can be used instead of
`cc_session`, and subsequent requests may supply the ID in that header.

The project, backing model, `reasoning_effort`, capability mode, and session mode are fixed for the lifetime
of a persistent session. Requests for the same session are serialized by
rejecting concurrent turns with HTTP 409. Idle sessions are closed after
`session_idle_timeout_secs`; capacity is limited by
`max_persistent_sessions`. Sessions do not survive a cc-connect daemon restart.

## Execution model

Ordinary HTTP requests start a fresh agent session. OpenAI clients should send
the full conversation in `messages`, as they normally do for stateless Chat
Completions requests. When the persistent extension is explicitly requested,
the gateway retains one isolated agent runtime and sends only new conversation
messages to it. The gateway converts messages into a role-labelled prompt and
streams agent text events back as Chat Completion chunks.

The configured alias (for example `codex-cli` or `qoder-cli`) uses the project's default agent model and reasoning effort. Selecting a native model returned by `/v1/models` overrides the backing agent model for that request. `reasoning_effort` accepts a value advertised by the selected model. The bundled Codex agent supports `low`, `medium`, `high`, `xhigh`, and `max`; Qoder also advertises `auto`, `none`, and `ultracode` when supported by the installed CLI.

Projects used only by the OpenAI-compatible gateway may omit
`[[projects.platforms]]` when they are referenced by `[openai_gateway.models]`.
This avoids opening duplicate IM connections for a second agent that shares the
same workspace. Agents implementing request-scoped options, including Codex and
Qoder, expose their selectable native models and reasoning efforts without
mutating the defaults used by other requests.

Qoder model discovery is prewarmed in the background and persisted per
cc-connect project. On the first-ever cold start the gateway can immediately
return Qoder's built-in Defaults while discovery runs; after that refresh,
subsequent process starts return Defaults, New Models, and Custom models from
the persistent cache on the first `/v1/models` request.

Overrides are isolated to the HTTP or persistent session. They do not change
the project's persisted defaults and do not affect Feishu sessions or other
gateway sessions.

## Current limitations

- text input and output only
- `n` must be omitted or set to `1`
- function/tool calling is not supported
- image, audio, and file content parts are not supported
- structured output and log probabilities are not supported
- unexpected interactive permission requests are denied because the HTTP protocol has no approval round trip

Unsupported features return an OpenAI-shaped `invalid_request_error` instead of being silently ignored.
