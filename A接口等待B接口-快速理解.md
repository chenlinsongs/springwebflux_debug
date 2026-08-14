# A接口等待B接口 - 快速理解

## 🎯 你的需求（1句话）

> A接口等待，直到B接口发送数据后才返回

---

## 💡 核心原理（3句话）

1. **A接口**：创建 `Mono.create()`，返回一个**不发射数据**的 Mono
2. **存储**：将 `MonoSink` 存到 `Map` 中
3. **B接口**：从 `Map` 取出 `MonoSink`，调用 `sink.success(data)` 发射数据

---

## 🔧 核心代码（20行）

```java
// 存储等待的 sink
private ConcurrentHashMap<String, MonoSink<String>> sinks = new ConcurrentHashMap<>();

// A接口：等待数据
@GetMapping("/wait/{id}")
public Mono<String> waitForData(@PathVariable String id) {
    return Mono.create(sink -> {
        sinks.put(id, sink);  // 存储sink
        sink.onDispose(() -> sinks.remove(id));  // 清理
    })
    .timeout(Duration.ofSeconds(30));  // 超时
}

// B接口：发送数据
@PostMapping("/send/{id}")
public Mono<String> sendData(@PathVariable String id, @RequestBody String data) {
    MonoSink<String> sink = sinks.get(id);  // 取出sink
    if (sink != null) {
        sink.success(data);  // 发射数据，触发A接口返回
        sinks.remove(id);
    }
    return Mono.just("OK");
}
```

---

## 📊 执行流程（可视化）

### 时间线

```
时间   终端1（A接口）              终端2（B接口）
───────────────────────────────────────────────────────
0秒    curl /wait/123
       ↓
       创建 Mono.create()
       ↓
       存储 sink 到 Map
       ↓
       返回 Mono（不发射数据）
       ↓
       框架 subscribe
       ↓
       [等待中...]
       
       
5秒                               curl -X POST /send/123
                                  ↓
                                  从 Map 取出 sink
                                  ↓
                                  调用 sink.success(data)
       ↓
       Mono 发射数据
       ↓
       onNext 回调
       ↓
       返回给客户端 ✓
```

### 数据流向

```
客户端                A接口                   Map                  B接口
  |                    |                      |                     |
  |---GET /wait/123--->|                      |                     |
  |                    |--存储 sink---------->|                     |
  |                    | [等待中...]          |                     |
  |                    |                      |                     |
  |                    |                      |<--取出 sink---------|<---POST /send/123
  |                    |<--success(data)------|                     |
  |<--返回数据---------|                      |                     |
  |                    |                      |                     |
```

---

## 🧪 测试（2个终端，2条命令）

### 终端1：请求A接口

```bash
curl http://localhost:8080/event-bridge/wait/123
```

**现象**：请求挂起，等待中...

### 终端2：请求B接口

```bash
curl -X POST http://localhost:8080/event-bridge/send/123 \
  -H "Content-Type: text/plain" \
  -d "Hello World"
```

**现象**：
- 终端2返回：`✅ 数据已发送给 A 接口`
- **终端1立即返回**：`Hello World`

---

## 🔍 关键对象

### Mono.create()

```java
Mono.create(sink -> {
    // sink 是一个 MonoSink 对象
    // 可以手动控制何时发射数据
})
```

### MonoSink

```java
MonoSink<String> sink = ...

sink.success(data);   // 发射数据（onNext + onComplete）
sink.error(error);    // 发射错误
sink.onDispose(() -> {
    // 客户端断开连接时的清理逻辑
});
```

### 为什么用 Map 存储？

```
A接口创建 sink → 需要保存起来
B接口需要访问这个 sink → 从 Map 中取出
通过 ID 匹配 → Map<String, MonoSink>
```

---

## 💭 常见疑问

### Q1：为什么A接口不立即返回？

**A**：因为 `Mono.create()` 创建的 Mono **没有立即发射数据**
- 只有当调用 `sink.success(data)` 时才发射数据
- B接口调用 `sink.success(data)` → A接口才返回

### Q2：框架会等待多久？

**A**：
- 默认：一直等待（直到客户端断开）
- 设置超时：`.timeout(Duration.ofSeconds(30))` → 30秒后自动返回

### Q3：如果B接口一直不发送怎么办？

**A**：
1. 如果设置了超时 → 超时后返回默认值
2. 如果没设置超时 → 一直等待（直到客户端断开）
3. 客户端断开 → `sink.onDispose()` 清理资源

### Q4：多个A接口同时等待会怎样？

**A**：
- **方式1（带ID）**：用不同ID，互不影响
  ```
  A1等待ID=111 → B发送ID=222 → A1继续等待
  A2等待ID=222 → B发送ID=222 → A2返回 ✓
  ```

- **方式2（多播）**：B发送一次，所有A同时返回
  ```
  A1、A2、A3都等待 → B广播 → A1、A2、A3同时返回 ✓
  ```

### Q5：这和轮询有什么区别？

**区别**：

| 特性 | 轮询（Polling） | A等待B（长连接） |
|------|----------------|------------------|
| **原理** | 客户端反复请求 | 请求挂起等待 |
| **网络开销** | 大（多次请求） | 小（一次请求） |
| **实时性** | 差（有延迟） | 好（立即返回） |
| **服务器压力** | 大 | 小 |

**轮询**：
```
客户端 → 请求 → 无数据 → 返回
  ↓
等待1秒
  ↓
客户端 → 请求 → 无数据 → 返回
  ↓
等待1秒
  ↓
客户端 → 请求 → 有数据 → 返回 ✓
```

**A等待B**：
```
客户端 → 请求 → [等待...等待...] → 有数据 → 返回 ✓
```

---

## 🎯 四种实现方式速查

| 方式 | A接口 | B接口 | 特点 |
|------|-------|-------|------|
| **方式1** | `/wait/{id}` | `/send/{id}` | 一对一，精确匹配 |
| **方式2** | `/wait-multicast` | `/broadcast` | 一对多，同时收到 |
| **方式3** | `/wait-queue` | `/enqueue` | 排队，先进先出 |
| **方式4** | `/wait-replay` | `/replay` | 缓存，立即获取 |

---

## 📝 实际应用

### 场景1：订单支付通知

```
用户下单 → 前端请求 A(/wait/订单ID)
          ↓
       [等待支付...]
          ↓
用户支付成功 → 支付回调触发 B(/send/订单ID)
          ↓
       前端收到通知 → 跳转成功页面
```

### 场景2：长任务进度

```
提交任务 → 前端请求 A(/wait/任务ID)
          ↓
       [等待任务完成...]
          ↓
任务完成 → 后台触发 B(/send/任务ID)
          ↓
       前端收到结果 → 显示结果
```

### 场景3：聊天消息

```
用户在线 → 请求 A(/wait-multicast)
          ↓
       [等待新消息...]
          ↓
有人发消息 → B(/broadcast)
          ↓
       所有在线用户收到消息
```

---

## ⚡ 核心知识点

### 1. Mono.create() 创建手动控制的 Mono

```java
Mono.create(sink -> {
    // 不立即发射数据
    // 等待手动调用 sink.success(data)
})
```

### 2. Map 存储 sink 用于跨接口通信

```java
// A接口
sinks.put(id, sink);

// B接口
MonoSink<String> sink = sinks.get(id);
sink.success(data);
```

### 3. 超时和清理避免资源泄漏

```java
.timeout(Duration.ofSeconds(30))  // 超时
.onDispose(() -> sinks.remove(id))  // 清理
```

---

## ✅ 理解检查

如果你能回答这些问题，说明你已经理解了：

1. ✅ A接口为什么不立即返回？
   - 因为 `Mono.create()` 不会立即发射数据

2. ✅ B接口如何触发A接口返回？
   - 调用 `sink.success(data)`

3. ✅ 为什么要用 Map 存储 sink？
   - A和B在不同的请求中，需要通过Map共享sink

4. ✅ 如果客户端断开会怎样？
   - 触发 `onDispose()` 清理资源

5. ✅ 如何避免一直等待？
   - 设置 `.timeout()`

---

## 🎓 记住这3点

```
1️⃣ Mono.create() 不立即发射
2️⃣ Map 存储 sink 共享
3️⃣ sink.success() 触发返回
```

---

## 🚀 现在就试试

```bash
# 启动应用
mvn spring-boot:run

# 终端1
curl http://localhost:8080/event-bridge/wait/123

# 终端2
curl -X POST http://localhost:8080/event-bridge/send/123 \
  -H "Content-Type: text/plain" \
  -d "Success!"
```

看到终端1返回 `Success!` 了吗？恭喜你理解了！🎉

