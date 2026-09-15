# OpenAI 兼容网关隔离文本模式

隔离文本模式用于把 cc-connect 的 `/v1` 入口限制为纯文本输入输出，同时根据 Agent 的真实能力提供两级保证：Qoder 为 `zero_tools`，底层工具 schema 数量为 0；Codex 为 `sandboxed_text`，保留内部工具 schema，但运行在独立空目录、只读沙箱、无审批、临时 thread 中，不能修改导演台项目。

## 配置

```toml
[openai_gateway]
enabled = true
listen = "127.0.0.1:9840"
token = "使用本机固定密码，不要提交到仓库"
access_log = true
text_only = true
persistent_sessions = true
session_idle_timeout_secs = 900
max_persistent_sessions = 16

[openai_gateway.models]
qoder-cli = "沃伦工厂-Qoder"
codex-cli = "沃伦工厂"
```

网关会在运行时识别并公开具体能力：

- Qoder 使用 `--tools ""`，每个网关会话均为 `zero_tools`。
- Codex 使用独立临时空目录、`sandbox=read-only`、`approvalPolicy=never`、`ephemeral=true`、空 `dynamicTools` 和禁止工具的 developer instruction，能力标记为 `sandboxed_text`。
- 其他无法提供任一级隔离的 Agent 才会从 `/v1/models` 隐藏；显式请求返回 `400 text_only_unsupported`。
- 这些设置只属于 OpenAI 网关。飞书和普通 CLI 沿用项目原有工作目录、`full-auto`/yolo 权限和工具能力。

## 客户端识别

`GET /v1/models` 返回的可用模型对象包含：

```json
{
  "id": "qoder-cli",
  "object": "model",
  "owned_by": "cc-connect",
  "cc_capability_mode": "zero_tools"
}
```

Codex 模型对应返回 `"cc_capability_mode": "sandboxed_text"`。

模型列表和补全响应都带有：

```text
X-CC-Capability-Mode: zero_tools
```

Chat Completion 响应头、非流式响应和 SSE 分块返回所选模型的具体模式：Qoder 为 `zero_tools`，Codex 为 `sandboxed_text`。`/v1/models` 的 HTTP 头仍以 `text_only` 表示整个端点只接受文本，具体保证以各模型对象为准。

请求仍然使用标准的文本 Chat Completions 形状：

```json
{
  "model": "qoder-cli",
  "reasoning_effort": "high",
  "messages": [
    {"role": "user", "content": "只返回最终文本结论。"}
  ]
}
```

`tools`、`tool_choice`、旧式 functions、图片、音频、文件和结构化输出参数仍会被网关拒绝，不会静默忽略。

## Session 复用

隔离文本模式兼容 `cc_session` 和 `cc_session_mode: "warm_reset"`。Session 会绑定项目、模型、推理强度、能力模式及 Session 模式；任一项变化都必须创建新 Session。导演台这种每次发送完整独立任务的调用方应使用 `warm_reset`，详见 [warm_reset 接入说明](openai-gateway-warm-reset.zh-CN.md)。

## 验收

部署后至少检查：

1. 未带固定 Bearer 密码访问 `/v1/models` 返回 401。
2. 带密码访问 `/v1/models` 同时看到 Qoder 和 Codex；前者标记 `zero_tools`，后者标记 `sandboxed_text`。
3. Codex 实际 thread 参数为独立空 `cwd`、`read-only`、`approvalPolicy=never`、`ephemeral=true` 和空动态工具列表，且不指向导演台目录。
4. Qoder 实际进程参数中存在 `--tools` 后跟空 argv 值；Qoder 初始化事件或日志显示工具 schema 数量为 0。
5. 两个后端的普通和复杂文本请求都能完成；Codex 即使保留内部 schema，也不能写入项目或进入交互审批。
6. 飞书项目的权限配置仍为 `full-auto`，其正常工具能力不受影响。

Qoder 对 `--tools ""` 的定义见 [Qoder CLI Reference](https://docs.qoder.com/cli/cli-reference)。OpenAI Chat Completions 的文本请求形状见 [OpenAI API Reference](https://developers.openai.com/api/reference/cli/resources/chat/subresources/completions)。
