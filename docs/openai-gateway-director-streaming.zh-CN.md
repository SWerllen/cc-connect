# 导演台接入 cc-connect SSE 与推理进度

本文面向调用 `http://127.0.0.1:9840/v1/chat/completions` 的导演台服务。目标是让长时间生成任务实时显示模型进度，同时保持最终 JSON 的完整性、任务隔离和可取消性。

## 1. 请求格式

```json
{
  "model": "qoder-cli/GLM-5.3",
  "reasoning_effort": "medium",
  "stream": true,
  "stream_options": {
    "include_usage": true,
    "include_reasoning": true
  },
  "cc_session_mode": "warm_reset",
  "messages": [
    {
      "role": "user",
      "content": "这里放置本次任务的完整、自包含提示词。只输出一个 JSON 对象。"
    }
  ]
}
```

请求头：

```http
Authorization: Bearer <本机固定密钥>
Content-Type: application/json
Accept: text/event-stream
```

注意：

- `stream: true` 才会启用 SSE。
- `stream_options.include_reasoning: true` 才会输出推理进度；默认关闭，避免无意暴露中间信息。
- `cc_session_mode: "warm_reset"` 表示复用 Agent 进程，但每轮完成后清除对话历史。每次请求都必须携带完整、自包含的任务上下文。
- 不要把固定密钥写入浏览器代码；由导演台 Node 服务端调用网关。

## 2. SSE 数据格式

响应类型为：

```http
Content-Type: text/event-stream
```

回答正文：

```text
data: {"choices":[{"index":0,"delta":{"content":"{\"version\":"},"finish_reason":null}],...}
```

可公开的推理摘要或进度：

```text
data: {"choices":[{"index":0,"delta":{"reasoning_content":"正在检查镜头时间边界"},"finish_reason":null}],...}
```

结束帧：

```text
data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],...}
data: {"choices":[],"usage":{"prompt_tokens":123,"completion_tokens":456,"total_tokens":579},...}
data: [DONE]
```

协议不变量：

- `delta.content` 是最终回答的增量片段，按顺序拼接后才能 `JSON.parse`。
- `delta.reasoning_content` 只用于任务中心的进度展示，绝不能拼入最终 JSON。
- Qoder 的 `redacted_thinking` 永不转发。
- 模型不一定产生推理进度；即使启用了 `include_reasoning`，也必须允许只有正文流。
- HTTP 连接成功后发生的 Agent 错误会作为 SSE `data: {"error": ...}` 帧发送，随后发送 `[DONE]`。

## 3. Node.js 参考实现

```js
export async function streamDirectorJson({
  baseUrl = 'http://127.0.0.1:9840/v1',
  apiKey,
  model,
  reasoningEffort = 'medium',
  prompt,
  signal,
  onReasoning = () => {},
  onContent = () => {},
}) {
  const response = await fetch(`${baseUrl}/chat/completions`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
      Accept: 'text/event-stream',
    },
    body: JSON.stringify({
      model,
      reasoning_effort: reasoningEffort,
      stream: true,
      stream_options: {
        include_usage: true,
        include_reasoning: true,
      },
      cc_session_mode: 'warm_reset',
      messages: [{ role: 'user', content: prompt }],
    }),
    signal,
  });

  if (!response.ok) {
    throw new Error(`cc-connect HTTP ${response.status}: ${await response.text()}`);
  }
  if (!response.body) throw new Error('cc-connect 未返回 SSE body');

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let content = '';
  let usage = null;
  let completed = false;

  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value || new Uint8Array(), { stream: !done });

    let separator;
    while ((separator = /\r?\n\r?\n/.exec(buffer))) {
      const boundary = separator.index;
      const block = buffer.slice(0, boundary).replace(/\r/g, '');
      buffer = buffer.slice(boundary + separator[0].length);

      for (const line of block.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const data = line.slice(5).trimStart();
        if (data === '[DONE]') {
          completed = true;
          continue;
        }

        const event = JSON.parse(data);
        if (event.error) throw new Error(event.error.message || 'cc-connect 流式请求失败');

        const delta = event.choices?.[0]?.delta || {};
        if (typeof delta.reasoning_content === 'string') {
          onReasoning(delta.reasoning_content);
        }
        if (typeof delta.content === 'string') {
          content += delta.content;
          onContent(delta.content, content);
        }
        if (event.usage) usage = event.usage;
      }
    }

    if (done) break;
  }

  if (!completed) throw new Error('cc-connect SSE 在 [DONE] 前断开');

  const cleaned = content
    .replace(/^```(?:json)?\s*/i, '')
    .replace(/\s*```$/, '')
    .trim();
  if (!cleaned) throw new Error('模型没有返回正文');

  return { parsed: JSON.parse(cleaned), rawContent: content, usage };
}
```

## 4. 导演台任务中心建议

一次任务维护两个独立缓冲区：

- `reasoningPreview`：滚动显示 `reasoning_content`，可限制为最近 2,000～4,000 字符。
- `contentBuffer`：完整保存 `content`，用于最终 JSON 解析和审计。

状态建议：

1. 收到 HTTP 200：`模型已连接，等待首个增量`。
2. 收到 reasoning：`模型正在推理`，刷新可见进度和最后活动时间。
3. 收到 content：`模型正在生成结果`，显示已接收字符数。
4. 收到 `finish_reason: stop`：`模型输出完成，正在校验 JSON`。
5. 收到 `[DONE]` 且 JSON 校验成功：任务完成。

不要用“请求已经持续多久”代替“最后一次收到增量距今多久”。建议同时记录：

- `startedAt`
- `firstChunkAt`
- `lastChunkAt`
- `reasoningChars`
- `contentChars`
- `finishReason`

## 5. 超时、取消与重试

- 使用一个覆盖整个导演任务的总时间预算，不要让每次自动重试各自拥有完整预算。
- 只要持续收到 SSE 增量，就说明连接仍有活动；可以采用“总预算 + 空闲超时”双重判断。
- 用户取消时中止 `AbortController`，不要继续后台重试。
- 超时不应自动连续重试三次。默认一次失败后向用户提供“重试”或“使用降级结果”的明确选择。
- 如果在收到部分 `content` 后断线，不要将半截 JSON 写入项目数据。
- `warm_reset` 请求失败后，不应假设旧会话仍可用；下一次任务仍发送完整上下文。

推荐初始值：

| 配置 | 建议值 |
|---|---:|
| 总时间预算 | 240 秒 |
| 无增量空闲超时 | 90 秒 |
| 自动重试 | 0 次 |
| reasoning 展示缓存 | 最近 4,000 字符 |
| content 审计上限 | 按现有 AI 审计策略 |

## 6. 兼容与降级

- 旧客户端继续使用非流式请求，不受本扩展影响。
- 如果调用方不能识别 `reasoning_content`，忽略该字段即可。
- 若不需要推理展示，删除 `include_reasoning` 或设为 `false`；正文仍可流式输出。
- 该字段是 cc-connect 的 OpenAI 兼容扩展，不应假设所有第三方 OpenAI 兼容服务都支持。

## 7. 验收清单

- 能逐块收到 `delta.content`，最终拼接并解析为一个 JSON 对象。
- 启用后能接收 `delta.reasoning_content`，但它不会污染最终 JSON。
- 未启用 `include_reasoning` 时不会出现推理字段。
- Qoder 的脱敏推理不会出现在 SSE、日志或最终结果中。
- 用户取消能立即终止读取，不会触发后台自动重试。
- 连接中断时不写入半成品项目数据。
- `warm_reset` 完成后响应包含正常的结束帧和 `[DONE]`。
