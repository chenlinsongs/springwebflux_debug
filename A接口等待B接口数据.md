# A接口等待B接口数据

## 🎯 场景说明

你的需求：
1. **客户端请求 A 接口** → A 接口不立即返回，处于等待状态
2. **用 Postman 请求 B 接口** → B 接口接收数据
3. **A 接口收到数据** → 返回给客户端

```
客户端                A接口                 B接口
  |                    |                     |
  |----请求 A--------->|                     |
  |                    | (等待中...)         |
  |                    |                     |
  |                    |                     |<---Postman发送数据
  |                    |<----触发数据--------|
  |<---返回数据--------|                     |
  |                    |                     |
```

---

## 💡 实现原理

使用 **Reactor Sinks** 作为 A 和 B 之间的数据桥梁。

### 核心概念

```java
// Sink：可以手动发射数据的 Mono/Flux
Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

// A 接口：订阅 Sink，等待数据
@GetMapping("/A")
public Mono<String> getA() {
    return sink.asFlux().next();  // 等待第一个数据
}

// B 接口：发送数据到 Sink
@PostMapping("/B")
public Mono<String> postB(@RequestBody String data) {
    sink.tryEmitNext(data);  // 发射数据
    return Mono.just("OK");
}
```

---

## 📝 实现方式对比

### 方式1：简单版 - 单次请求

```
场景：
- 一个 A 请求对应一个 B 请求
- A 请求后，等待一次 B 的数据
- 收到数据后，A 返回并结束
```

### 方式2：多播版 - 多个A等待同一个B

```
场景：
- 多个 A 请求同时等待
- B 发送一次数据
- 所有等待的 A 都收到数据并返回
```

### 方式3：队列版 - 排队等待

```
场景：
- 多个 A 请求排队等待
- B 发送多次数据
- 每个 A 依次获取一个数据
```

### 方式4：带ID版 - 精确匹配

```
场景：
- A 请求带 ID
- B 发送数据时指定 ID
- 只有匹配 ID 的 A 才返回
```

---

## 🔧 完整实现示例

见后续的 `EventBridgeController.java`

---

## ⚙️ 使用场景

### 1. 长轮询（Long Polling）

```
客户端轮询消息，有消息时立即返回
```

### 2. 异步任务状态查询

```
提交任务后，轮询任务状态，完成时返回结果
```

### 3. 服务端推送

```
客户端连接后等待，服务端有数据时推送
```

### 4. 消息通知

```
等待通知消息，收到通知后返回
```

---

## 📊 工作流程

### 简单版流程

```
1. 客户端请求 A 接口
   ↓
2. A 接口创建 Mono.create()
   ↓
3. 将 MonoSink 存储到 Map 中
   ↓
4. 返回 Mono（不发射数据，等待）
   ↓
5. Postman 请求 B 接口，带数据
   ↓
6. B 接口从 Map 取出 MonoSink
   ↓
7. 调用 sink.success(data)
   ↓
8. A 接口的 Mono 收到数据
   ↓
9. 返回给客户端
```

### 多播版流程

```
1. 创建全局 Sinks.Many
   ↓
2. 客户端1请求 A 接口 → 订阅 Sink
   客户端2请求 A 接口 → 订阅 Sink
   客户端3请求 A 接口 → 订阅 Sink
   ↓
3. 所有客户端都在等待...
   ↓
4. Postman 请求 B 接口，带数据
   ↓
5. B 接口调用 sink.tryEmitNext(data)
   ↓
6. 所有订阅的客户端同时收到数据
   ↓
7. 所有客户端返回
```

---

## ⚠️ 注意事项

### 1. 超时处理

```java
// A 接口应该设置超时
return sink.asFlux()
    .next()
    .timeout(Duration.ofSeconds(30))  // 30秒超时
    .onErrorReturn("超时，未收到数据");
```

### 2. 内存管理

```java
// 使用 Map 存储 Sink 时，要及时清理
// 避免内存泄漏
map.put(id, sink);

// 超时或完成后删除
sink.success(data);
map.remove(id);
```

### 3. 并发安全

```java
// 使用线程安全的容器
private final ConcurrentHashMap<String, MonoSink<String>> sinks 
    = new ConcurrentHashMap<>();
```

### 4. 错误处理

```java
// B 接口应该检查是否有等待的请求
if (sink != null) {
    sink.success(data);
} else {
    // 没有等待的请求
}
```

---

## 🎯 关键代码模板

### A 接口（等待数据）

```java
@GetMapping("/A/{id}")
public Mono<String> waitForData(@PathVariable String id) {
    return Mono.create(sink -> {
        // 存储 sink，等待数据
        sinkMap.put(id, sink);
        
        // 设置清理逻辑
        sink.onDispose(() -> {
            sinkMap.remove(id);
            System.out.println("客户端断开，清理 sink");
        });
    })
    // 设置超时
    .timeout(Duration.ofSeconds(30))
    .onErrorReturn("超时，未收到数据");
}
```

### B 接口（发送数据）

```java
@PostMapping("/B/{id}")
public Mono<String> sendData(@PathVariable String id, @RequestBody String data) {
    MonoSink<String> sink = sinkMap.get(id);
    
    if (sink != null) {
        // 发送数据
        sink.success(data);
        sinkMap.remove(id);
        return Mono.just("数据已发送给 A 接口");
    } else {
        return Mono.just("没有等待的 A 接口");
    }
}
```

---

## 📝 测试步骤

### 简单版测试

```bash
# 终端1：请求 A 接口（等待）
curl http://localhost:8080/event-bridge/wait/123

# 此时请求会挂起，等待数据...

# 终端2：用 Postman 或 curl 请求 B 接口（发送数据）
curl -X POST http://localhost:8080/event-bridge/send/123 \
  -H "Content-Type: text/plain" \
  -d "Hello from B"

# 终端1 立即收到响应：
# Hello from B
```

### 多播版测试

```bash
# 终端1：请求 A 接口
curl http://localhost:8080/event-bridge/wait-multicast

# 终端2：同时请求 A 接口
curl http://localhost:8080/event-bridge/wait-multicast

# 终端3：同时请求 A 接口
curl http://localhost:8080/event-bridge/wait-multicast

# 三个请求都在等待...

# 终端4：用 Postman 发送数据
curl -X POST http://localhost:8080/event-bridge/broadcast \
  -H "Content-Type: text/plain" \
  -d "Broadcast message"

# 终端1、2、3 同时收到响应：
# Broadcast message
```

---

## ✅ 总结

### 核心要点

1. **使用 Mono.create() 或 Sinks**
   ```
   创建可以手动控制的 Mono
   ```

2. **A 接口订阅，B 接口发射**
   ```
   A: return Mono.create(sink -> ...)
   B: sink.success(data)
   ```

3. **存储 Sink 到 Map**
   ```
   A 接口存储 sink
   B 接口从 Map 取出并发射数据
   ```

4. **设置超时**
   ```
   .timeout(Duration.ofSeconds(30))
   ```

5. **清理资源**
   ```
   sink.onDispose(() -> map.remove(id))
   ```

---

## 🎓 记忆口诀

```
A接口等待不返回，
Mono.create来帮忙。

Sink存入Map等待，
B接口来了发数据。

success方法触发返回，
客户端收到笑开颜。

超时清理要记牢，
内存泄漏是大忌。
```

