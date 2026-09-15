# cc-connect OpenAI 网关 Session 复用接入说明

本文面向调用 `http://127.0.0.1:9840/v1/chat/completions` 的开发者，说明如何复用同一个 Codex 或 Qoder 运行时，避免每次请求都重新初始化 CLI 进程。

本文描述会保留聊天历史的 `history` 模式。独立任务应使用 [常驻进程、请求隔离的 `warm_reset` 模式](openai-gateway-warm-reset.zh-CN.md)。

## 1. 判断是否真正复用

模型名称相同不代表复用了 session。客户端必须在第一轮显式创建 session，并在后续轮次回传网关返回的 `cc_session_id`。

访问日志中的 `session_action` 是最终判断依据：

| `session_action` | 含义 |
| --- | --- |
| `stateless` | 无状态请求；每次都会启动新的 Agent 运行时 |
| `create` | 创建了新的持久 session |
| `reuse` | 成功定位并尝试复用已有持久 session；结合 HTTP 2xx 判断请求成功 |

一次正常的连续对话应表现为：

```text
session_action=create session_id=ccs_xxx
session_action=reuse  session_id=ccs_xxx
session_action=reuse  session_id=ccs_xxx
```

## 2. 请求协议

### 第一轮：创建 session

```http
POST /v1/chat/completions
Authorization: Bearer <本地固定 Token>
Content-Type: application/json
```

```json
{
  "model": "qoder-cli/Qwen3.8-Max",
  "reasoning_effort": "low",
  "cc_session": true,
  "messages": [
    {"role": "user", "content": "分析这个项目的目录结构"}
  ]
}
```

成功响应会在两个位置返回同一个 session ID：

- JSON 响应正文的 `cc_session_id`
- HTTP 响应头的 `X-CC-Session-ID`

示例：

```json
{
  "id": "chatcmpl-xxx",
  "object": "chat.completion",
  "model": "qoder-cli/Qwen3.8-Max",
  "cc_session_id": "ccs_0123456789abcdef",
  "choices": [
    {
      "index": 0,
      "message": {"role": "assistant", "content": "……"},
      "finish_reason": "stop"
    }
  ]
}
```

### 后续轮次：复用 session

把第一轮返回的 ID 放进 `cc_session_id`：

```json
{
  "model": "qoder-cli/Qwen3.8-Max",
  "reasoning_effort": "low",
  "cc_session_id": "ccs_0123456789abcdef",
  "messages": [
    {"role": "user", "content": "继续检查入口文件"}
  ]
}
```

也可以通过请求头传递：

```http
X-CC-Session-ID: ccs_0123456789abcdef
```

创建新 session 时，还可以使用 `X-CC-Session-ID: new`。正文参数更直观，建议普通客户端优先使用正文参数。

## 3. Node.js 推荐写法

下面的封装使用 Node.js 18 及以上版本内置的 `fetch`。每个 `CCConnectSession` 实例对应一个独立对话，不要让不同用户共享同一个实例。

```js
class CCConnectSession {
  constructor({
    baseUrl = "http://127.0.0.1:9840/v1",
    apiKey,
    model = "qoder-cli/Qwen3.8-Max",
    reasoningEffort = "low",
  }) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiKey = apiKey;
    this.model = model;
    this.reasoningEffort = reasoningEffort;
    this.sessionId = null;
  }

  async chat(content) {
    const body = {
      model: this.model,
      reasoning_effort: this.reasoningEffort,
      messages: [{ role: "user", content }],
    };

    if (this.sessionId) {
      body.cc_session_id = this.sessionId;
    } else {
      body.cc_session = true;
    }

    const response = await fetch(`${this.baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(
        `cc-connect ${response.status}: ${data?.error?.code ?? "unknown_error"} - ${data?.error?.message ?? "request failed"}`,
      );
    }

    const returnedSessionId =
      data.cc_session_id ?? response.headers.get("x-cc-session-id");
    if (!returnedSessionId) {
      throw new Error("cc-connect did not return cc_session_id");
    }

    this.sessionId = returnedSessionId;
    return data.choices[0].message.content;
  }

  reset() {
    // 下一次 chat() 会创建全新的持久 session。
    this.sessionId = null;
  }
}

const session = new CCConnectSession({
  apiKey: process.env.CC_CONNECT_API_KEY,
});

console.log(await session.chat("先检查项目结构"));
console.log(await session.chat("继续分析刚才发现的入口"));
```

如果使用 `openai` Node SDK，自定义字段不在官方类型定义中，可以使用一个扩展对象：

```ts
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://127.0.0.1:9840/v1",
  apiKey: process.env.CC_CONNECT_API_KEY,
});

let sessionId: string | undefined;

async function chat(content: string) {
  const request: any = {
    model: "codex-cli",
    reasoning_effort: "low",
    messages: [{ role: "user", content }],
    ...(sessionId
      ? { cc_session_id: sessionId }
      : { cc_session: true }),
  };

  const response: any = await client.chat.completions.create(request);
  sessionId = response.cc_session_id;
  return response.choices[0].message.content;
}
```

## 4. 消息历史的两种传法

推荐使用增量模式：每次只发送本轮新消息。Codex 或 Qoder 进程已经保留了之前的上下文，网关不会重复注入历史。

```json
"messages": [{"role": "user", "content": "继续"}]
```

网关也接受完整 OpenAI `messages` 历史，但历史必须精确延续上一次请求和响应。网关验证通过后只会向 Agent 转发新增后缀。历史被修改、截断或与网关记录不一致时，会返回 `409 session_history_mismatch`。

## 5. 模型、推理强度与并发约束

一个持久 session 创建后，会绑定以下配置：

- cc-connect 项目
- Agent 模型
- `reasoning_effort`

后续复用时必须继续发送相同的 `model` 和 `reasoning_effort`。如需切换模型或推理强度，应创建新 session。

同一个 session 同一时间只允许处理一个请求。客户端应对同一 session 串行发送消息；并发请求会收到 `409 session_busy`。不同用户或不同对话应分别维护各自的 session ID。

## 6. 生命周期与错误处理

当前本机配置为：

```toml
[openai_gateway]
access_log = true
persistent_sessions = true
session_idle_timeout_secs = 900
max_persistent_sessions = 16
```

session 是网关内存状态：

- 空闲 15 分钟后自动关闭。
- cc-connect 守卫重启后全部失效。
- session 失效意味着运行时上下文也已丢失；创建新 session 不会恢复旧上下文。

常见错误：

| HTTP 状态 | 错误码 | 建议处理 |
| --- | --- | --- |
| 400 | `persistent_sessions_disabled` | 检查网关配置是否启用持久 session |
| 404 | `session_not_found` | session 已过期或网关已重启；创建新 session |
| 409 | `session_busy` | 对同一 session 串行请求，稍后重试 |
| 409 | `session_options_mismatch` | 模型或推理强度发生变化；创建新 session |
| 409 | `session_history_mismatch` | 改用增量消息，或创建新 session |
| 410 | `session_closed` | Agent 进程已退出；创建新 session |
| 429 | `session_capacity_exceeded` | 等待空闲 session 回收，或调整容量 |

## 7. 流式请求

`stream: true` 同样支持 session 复用。创建 session 时，可从 `X-CC-Session-ID` 响应头读取 ID；第一个 SSE 数据块也包含 `cc_session_id`。后续流式请求回传该 ID 即可。

不要等整个流结束后才决定是否保存 ID；收到响应头后即可保存。但在流式请求完成前，不要并发复用同一 session。

## 8. 日志排查

查看最近 100 行访问记录：

```powershell
Get-Content C:\Users\58376\.cc-connect\logs\cc-connect.log -Tail 100 |
  Select-String "openai gateway access"
```

只看最后两次访问：

```powershell
Get-Content C:\Users\58376\.cc-connect\logs\cc-connect.log -Tail 500 |
  Select-String "openai gateway access" |
  Select-Object -Last 2
```

排查顺序：

1. 确认 HTTP `status` 为 2xx。
2. 第一轮应为 `session_action=create`，并出现 `session_id`。
3. 后续轮次应为 `session_action=reuse`，且 `session_id` 与第一轮相同。
4. 如果一直是 `stateless`，检查客户端是否真的把扩展字段序列化进 JSON 请求体。
5. 如果耗时仍高，比较同一 session 的多轮 `duration_ms`，再检查 Agent 自身网络或模型响应时间。

访问日志不会记录 Authorization、请求正文、提示词或回复正文。

## 9. 接入验收清单

- 第一轮发送了 `cc_session: true`。
- 第一轮保存了响应中的 `cc_session_id`。
- 后续轮次回传同一个 `cc_session_id`。
- 同一 session 的模型和推理强度保持不变。
- 同一 session 的请求已经串行化。
- 访问日志呈现 `create -> reuse -> reuse`。
- 日志中对应请求均为 HTTP 2xx。
