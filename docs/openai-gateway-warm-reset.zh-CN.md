# OpenAI 网关 `warm_reset` 接入说明

`warm_reset` 用于互相独立、但希望省去 Codex/Qoder CLI 重复启动开销的任务。它复用底层运行进程，每次请求结束后清空 Agent 侧对话上下文；它不复用上一轮聊天内容。

## 适用场景

- 每次请求都携带完整任务资料，例如导演生成、批处理和结构化 JSON 生成。
- 不允许联通测试、其他镜头或上一项任务污染当前结果。
- 希望保留模型、推理强度和工作目录相同的常驻运行进程。

需要连续对话时，继续使用默认的 `history` Session，参见 [Session 复用接入说明](openai-gateway-session-reuse.zh-CN.md)。

## 网关配置

```toml
[openai_gateway]
enabled = true
listen = "127.0.0.1:9840"
token = "使用本机固定密码，不要写入代码仓库"
access_log = true
text_only = true
persistent_sessions = true
session_idle_timeout_secs = 900
max_persistent_sessions = 16
```

修改配置或二进制后需要重启 cc-connect 守卫进程。Token 通过 `Authorization: Bearer ...` 发送。

`text_only = true` 会按 Agent 的真实能力提供隔离：Qoder 通过 `--tools ""` 启动并返回 `cc_capability_mode: "zero_tools"`；Codex 使用独立空目录、只读沙箱、`approvalPolicy=never`、临时 thread 和空动态工具列表，返回 `cc_capability_mode: "sandboxed_text"`。Codex 仍保留内置工具 schema，所以不能标记为零工具。这个配置只影响 OpenAI 网关，飞书和普通 CLI 的 `full-auto` 不变。

## 请求约束

`warm_reset` 请求必须满足：

- `messages` 恰好只有一项；
- 该项 `role` 必须为 `user`；
- `content` 必须是非空文本；
- 同一个 Session 的 `model`、`reasoning_effort`、能力模式和 cc-connect 项目保持不变；
- 同一个 Session 的请求串行发送。

原本的 system 规则、任务数据和输出约束应合并进这一条 user 消息。`response_format` 当前不受支持；请在提示词中要求只输出 JSON，并在调用端做 JSON Schema 或业务校验。

## 创建常驻运行实例

```http
POST http://127.0.0.1:9840/v1/chat/completions
Authorization: Bearer <本机固定密码>
Content-Type: application/json
```

```json
{
  "model": "qoder-cli",
  "reasoning_effort": "high",
  "cc_session": true,
  "cc_session_mode": "warm_reset",
  "messages": [
    {
      "role": "user",
      "content": "这是一次独立任务。不得依赖此前对话。\n\n[规则]\n只输出一个 JSON 对象。\n\n[当前项目数据]\n...\n\n[任务]\n生成导演执行序列。"
    }
  ]
}
```

成功响应包含：

```json
{
  "cc_session_id": "ccs_...",
  "cc_session_mode": "warm_reset",
  "cc_context_reset": "ok"
}
```

响应头同时包含：

```text
X-CC-Session-ID: ccs_...
X-CC-Session-Mode: warm_reset
X-CC-Context-Reset: ok
```

只有 `cc_context_reset` 为 `ok` 时才可以保存和复用该 Session ID。

## 后续请求

后续请求仍只发送本次任务，不发送历史：

```json
{
  "model": "qoder-cli",
  "reasoning_effort": "high",
  "cc_session_id": "ccs_第一轮返回的值",
  "cc_session_mode": "warm_reset",
  "messages": [
    {
      "role": "user",
      "content": "这是另一项独立任务。这里包含其全部规则和当前数据。"
    }
  ]
}
```

客户端必须等待上一请求完整结束后再复用同一个 ID。并发复用会返回 `409 session_busy`。

## Node.js 示例

```js
class WarmResetClient {
  constructor({ baseUrl, apiKey, model, reasoningEffort }) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiKey = apiKey;
    this.model = model;
    this.reasoningEffort = reasoningEffort;
    this.sessionId = null;
    this.queue = Promise.resolve();
  }

  run(content) {
    const current = this.queue.catch(() => undefined).then(() => this.#run(content));
    this.queue = current.then(() => undefined, () => undefined);
    return current;
  }

  async #run(content) {
    const body = {
      model: this.model,
      reasoning_effort: this.reasoningEffort,
      cc_session_mode: "warm_reset",
      messages: [{ role: "user", content }],
      ...(this.sessionId
        ? { cc_session_id: this.sessionId }
        : { cc_session: true }),
    };

    let response = await fetch(`${this.baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
    let data = await response.json();

    if (["session_not_found", "session_closed", "session_options_mismatch"].includes(data?.error?.code)) {
      this.sessionId = null;
      delete body.cc_session_id;
      body.cc_session = true;
      response = await fetch(`${this.baseUrl}/chat/completions`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.apiKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });
      data = await response.json();
    }

    if (!response.ok) {
      throw new Error(`${response.status} ${data?.error?.code || "request_failed"}: ${data?.error?.message || ""}`);
    }

    if (data.cc_context_reset === "ok" && data.cc_session_id) {
      this.sessionId = data.cc_session_id;
    } else {
      // 本轮内容可用，但下轮必须创建干净实例。
      this.sessionId = null;
    }
    return data.choices[0].message.content;
  }
}
```

## 沃伦工厂导演台的当前接入

`D:\ai\storyboard-studio-dream` 已按上述协议接入：

- 仅对 localhost、回环地址和私有网络上的 OpenAI 兼容接口启用 `warm_reset`；远程 OpenAI API 保持标准请求体；
- 原请求里的 system、developer、user、assistant 文本会按原顺序合并成一条自包含的 user 消息；
- 所有本地生成任务通过一个进程级队列串行使用全局 runtime；
- 只有网关明确返回 `cc_context_reset: "ok"` 才保存 Session ID；重置失败时本轮生成结果仍可用，但下轮创建新 runtime；
- 模型、`reasoning_effort`、接口地址或认证身份变化时，自动放弃旧 runtime；
- runtime 空闲 14 分钟后由导演台主动轮换，网关自身仍按 `session_idle_timeout_secs` 清理资源。

因此这里的“全局 Session”准确含义是：全导演台共享一个常驻 CLI 运行实例，而不是共享一段不断增长的对话历史。

## Agent 实现

- Codex：保留同一个 `codex app-server` 进程，每轮完成后调用 `thread/start` 切换到新的临时 thread。网关 Session 始终使用独立空目录、只读沙箱和 `approvalPolicy=never`，标记为 `sandboxed_text`，不伪装成零工具。
- Qoder：保留同一个 stream-json 进程，每轮结束后发送 `rewind` 控制请求，并固定使用 `scope: "conversation"` 删除本轮对话。Qoder 的 transcript writer 必须保持启用，否则 CLI 无法执行 rewind；conversation 作用域不会撤销或回滚工作区文件。
- 任一 Agent 重置失败时，网关会关闭并淘汰该运行实例。非流式响应仍返回已生成内容，但不会返回可复用的 `cc_session_id`，且 `cc_context_reset` 为 `failed`。

## 错误处理

| HTTP / 错误码 | 含义 | 处理方式 |
| --- | --- | --- |
| 400 `warm_reset_requires_single_user_message` | 消息不是单条 user 文本 | 合并为一条完整 user 消息 |
| 400 `persistent_sessions_disabled` | 网关未启用持久实例 | 开启 `persistent_sessions` 或改用无状态请求 |
| 400 `text_only_unsupported` | 所选 Agent 不能提供零工具或沙箱文本隔离 | 改用 `/v1/models` 中仍可见的模型 |
| 404 `session_not_found` | 实例过期或守卫已重启 | 不带旧 ID，重新发送 `cc_session: true` |
| 409 `session_busy` | 同一实例仍在执行或重置 | 排队后重试 |
| 409 `session_options_mismatch` | 模型、强度、项目或模式变化 | 创建新实例 |
| 410 `session_closed` | Agent 进程已退出 | 创建新实例 |
| SSE `context_reset_failed` | 流式输出结束前重置失败 | 丢弃旧 ID，下次创建新实例 |

## 日志与验收

访问日志不会记录密码、提示词或回复正文。正常调用序列应出现：

```text
session_action=create session_mode=warm_reset context_reset=ok session_id=ccs_...
session_action=reuse  session_mode=warm_reset context_reset=ok session_id=ccs_...
capability_mode=zero_tools
# 或 capability_mode=sandboxed_text
```

验收时至少确认：

1. 两次请求的 `session_id` 相同，第二次为 `session_action=reuse`；
2. 两次请求均为 `context_reset=ok`；
3. 第二次任务无法回忆第一轮仅存在于提示词中的随机标记；
4. 日志中没有 `session_history_mismatch`；
5. 模型、推理强度或能力模式变化后创建了新的运行实例；
6. Qoder 启动参数包含 `--tools` 后跟空值，运行日志确认工具 schema 数量为 0。

本机 2026-09-06 的 Qoder 验收结果：首轮创建耗时约 13.3 秒，次轮复用约 4.6 秒；两轮均为 `context_reset=ok`，第二轮无法回忆首轮随机标记。具体耗时受模型服务和任务复杂度影响，不应把进程复用等同于固定的推理加速比例。
