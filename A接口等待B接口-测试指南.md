# A接口等待B接口数据 - 测试指南

## 🎯 你的场景

**需求**：
1. 客户端请求 **A 接口**，但不立即返回，处于**等待状态**
2. 用 **Postman** 或 **curl** 请求 **B 接口**发送数据
3. **A 接口**收到数据后，立即返回给客户端

```
客户端 ----请求A----> A接口（等待中...）
                        ↓
Postman ---数据---> B接口 → 触发A接口
                        ↓
客户端 <---返回数据--- A接口
```

---

## 🚀 快速开始

### 1. 启动应用

```bash
# 启动 Spring Boot 应用
mvn spring-boot:run

# 或运行 Main 类
```

### 2. 打开多个终端

你需要至少 **2个终端**：
- **终端1**：请求 A 接口（会挂起等待）
- **终端2**：请求 B 接口（触发数据）

---

## 📝 方式1：带ID的一对一等待（推荐入门）

### 场景说明

```
一个 A 请求对应一个 B 请求
通过 ID 精确匹配
```

### 测试步骤

#### 步骤1：终端1请求A接口（等待）

```bash
curl http://localhost:8080/event-bridge/wait/123
```

**现象**：
- 请求**挂起**，不返回
- 光标闪烁，等待中...

**控制台输出**：
```
=== A接口：等待数据，ID=123 ===
创建 Mono，等待数据...
已存储 sink 到 Map，当前等待数量：1
```

#### 步骤2：终端2请求B接口（发送数据）

```bash
curl -X POST http://localhost:8080/event-bridge/send/123 \
  -H "Content-Type: text/plain" \
  -d "Hello from B"
```

**现象**：
- 终端2 立即返回：`✅ 数据已发送给 A 接口，ID=123`
- **终端1 立即收到数据**：`Hello from B`

**控制台输出**：
```
=== B接口：接收数据，ID=123, data=Hello from B ===
找到等待的 A 接口，发送数据
A接口返回数据：Hello from B
```

### 多个ID同时等待

```bash
# 终端1：等待ID=111
curl http://localhost:8080/event-bridge/wait/111

# 终端2：等待ID=222
curl http://localhost:8080/event-bridge/wait/222

# 终端3：等待ID=333
curl http://localhost:8080/event-bridge/wait/333

# 终端4：发送给222
curl -X POST http://localhost:8080/event-bridge/send/222 \
  -H "Content-Type: text/plain" \
  -d "Data for 222"

# 结果：只有终端2收到数据，终端1和3仍在等待
```

### 超时测试

```bash
# 请求A接口，但不发送数据
curl http://localhost:8080/event-bridge/wait/999

# 等待30秒后，自动返回：
# 等待超时（30秒），未收到数据
```

### 查看等待状态

```bash
# 查看当前有多少个A接口在等待
curl http://localhost:8080/event-bridge/status

# 响应示例：
# 当前状态：
# - 等待中的 A 接口（带ID）：2 个
# - 等待 ID 列表：[111, 333]
```

---

## 📝 方式2：多播 - 一对多广播

### 场景说明

```
多个 A 请求同时等待
B 发送一次数据
所有 A 同时收到
```

### 测试步骤

#### 步骤1：开启3个终端，都请求A接口

```bash
# 终端1
curl http://localhost:8080/event-bridge/wait-multicast

# 终端2
curl http://localhost:8080/event-bridge/wait-multicast

# 终端3
curl http://localhost:8080/event-bridge/wait-multicast
```

**现象**：
- 3个终端都挂起
- 都在等待数据

**控制台输出**：
```
=== A接口：多播等待 ===
新的 A 接口开始等待
=== A接口：多播等待 ===
新的 A 接口开始等待
=== A接口：多播等待 ===
新的 A 接口开始等待
```

#### 步骤2：终端4发送广播

```bash
# 终端4
curl -X POST http://localhost:8080/event-bridge/broadcast \
  -H "Content-Type: text/plain" \
  -d "Broadcast message to all"
```

**现象**：
- 终端1、2、3 **同时收到**：`Broadcast message to all`
- 3个请求同时完成

**控制台输出**：
```
=== B接口：广播数据，data=Broadcast message to all ===
广播成功
A接口收到广播数据：Broadcast message to all
A接口收到广播数据：Broadcast message to all
A接口收到广播数据：Broadcast message to all
```

### 可视化流程

```
时间线：
0s:  终端1 请求 A -----> (等待中)
1s:  终端2 请求 A -----> (等待中)
2s:  终端3 请求 A -----> (等待中)
     
     [所有终端都在等待...]
     
5s:  终端4 请求 B -----> 广播数据
     
     ↓
     终端1 收到数据 ✓
     终端2 收到数据 ✓
     终端3 收到数据 ✓
```

---

## 📝 方式3：队列 - 排队处理

### 场景说明

```
多个 A 请求排队等待
B 每次发送数据，只有一个 A 收到
先进先出（FIFO）
```

### 测试步骤

#### 步骤1：开启3个终端排队

```bash
# 终端1（第1个）
curl http://localhost:8080/event-bridge/wait-queue

# 终端2（第2个）
curl http://localhost:8080/event-bridge/wait-queue

# 终端3（第3个）
curl http://localhost:8080/event-bridge/wait-queue
```

**现象**：
- 3个终端都在等待
- 排队顺序：1 → 2 → 3

#### 步骤2：终端4发送第1条数据

```bash
# 终端4
curl -X POST http://localhost:8080/event-bridge/enqueue \
  -H "Content-Type: text/plain" \
  -d "Message 1"
```

**现象**：
- **只有终端1** 收到：`Message 1`
- 终端2、3 仍在等待

#### 步骤3：终端4发送第2条数据

```bash
# 终端4
curl -X POST http://localhost:8080/event-bridge/enqueue \
  -H "Content-Type: text/plain" \
  -d "Message 2"
```

**现象**：
- **只有终端2** 收到：`Message 2`
- 终端3 仍在等待

#### 步骤4：终端4发送第3条数据

```bash
# 终端4
curl -X POST http://localhost:8080/event-bridge/enqueue \
  -H "Content-Type: text/plain" \
  -d "Message 3"
```

**现象**：
- **只有终端3** 收到：`Message 3`
- 所有终端都已返回

### 可视化流程

```
队列状态：

初始：[终端1, 终端2, 终端3]
      
B发送 "Message 1"
      ↓
      终端1 收到 → 出队
      [终端2, 终端3]

B发送 "Message 2"
      ↓
      终端2 收到 → 出队
      [终端3]

B发送 "Message 3"
      ↓
      终端3 收到 → 出队
      []（队列空）
```

---

## 📝 方式4：重播 - 缓存最后的值

### 场景说明

```
B 接口发送的数据会被缓存
新的 A 接口可以立即获取缓存的值
如果没有缓存，则等待
```

### 测试步骤

#### 场景A：先等待，后发送

```bash
# 终端1：A接口等待
curl http://localhost:8080/event-bridge/wait-replay

# 终端2：B接口发送
curl -X POST http://localhost:8080/event-bridge/replay \
  -H "Content-Type: text/plain" \
  -d "First message"

# 终端1收到：First message
```

#### 场景B：先发送，后等待（立即返回）

```bash
# 终端1：B接口先发送数据
curl -X POST http://localhost:8080/event-bridge/replay \
  -H "Content-Type: text/plain" \
  -d "Cached message"

# 终端2：A接口请求（立即返回缓存值）
curl http://localhost:8080/event-bridge/wait-replay

# 立即返回：Cached message（不需要等待！）
```

#### 场景C：多次发送，只缓存最后一个

```bash
# 发送第1条
curl -X POST http://localhost:8080/event-bridge/replay \
  -H "Content-Type: text/plain" \
  -d "Message 1"

# 发送第2条
curl -X POST http://localhost:8080/event-bridge/replay \
  -H "Content-Type: text/plain" \
  -d "Message 2"

# 发送第3条
curl -X POST http://localhost:8080/event-bridge/replay \
  -H "Content-Type: text/plain" \
  -d "Message 3"

# 新的A接口请求
curl http://localhost:8080/event-bridge/wait-replay

# 立即返回：Message 3（只返回最后一个）
```

---

## 📊 四种方式对比

| 方式 | 特点 | 适用场景 | 接口 |
|------|------|----------|------|
| **方式1：带ID** | 一对一，精确匹配 | 特定客户端等待特定数据 | `/wait/{id}` + `/send/{id}` |
| **方式2：多播** | 一对多，同时收到 | 通知所有在线客户端 | `/wait-multicast` + `/broadcast` |
| **方式3：队列** | 先进先出，逐个处理 | 任务分配，负载均衡 | `/wait-queue` + `/enqueue` |
| **方式4：重播** | 缓存最后值，可立即获取 | 最新状态查询 | `/wait-replay` + `/replay` |

---

## 🧪 实际应用场景

### 场景1：订单状态通知（方式1）

```
1. 用户下单后，前端请求 /wait/订单ID
2. 前端等待...
3. 后台处理完订单，调用 /send/订单ID
4. 前端立即收到通知
```

### 场景2：系统公告推送（方式2）

```
1. 多个在线用户请求 /wait-multicast
2. 管理员发布公告，调用 /broadcast
3. 所有在线用户同时收到公告
```

### 场景3：任务分配（方式3）

```
1. 多个worker请求 /wait-queue（等待任务）
2. 系统有新任务，调用 /enqueue
3. 第一个worker收到任务并处理
4. 重复2-3，直到所有任务分配完
```

### 场景4：实时股票价格（方式4）

```
1. 股票价格更新，调用 /replay（缓存最新价格）
2. 用户请求 /wait-replay
3. 立即返回最新价格（不需要等待）
```

---

## 🎨 可视化演示

### 方式1：带ID匹配

```
客户端A ----wait/111----> 等待
客户端B ----wait/222----> 等待
客户端C ----wait/333----> 等待

Postman ----send/222----> 触发
              ↓
         客户端B 收到 ✓
```

### 方式2：多播广播

```
客户端A ----wait-multicast----> 等待
客户端B ----wait-multicast----> 等待
客户端C ----wait-multicast----> 等待

Postman ----broadcast----> 触发
              ↓
         客户端A 收到 ✓
         客户端B 收到 ✓
         客户端C 收到 ✓
```

### 方式3：队列处理

```
客户端A ----wait-queue----> [A, B, C]
客户端B ----wait-queue----> [A, B, C]
客户端C ----wait-queue----> [A, B, C]

Postman ----enqueue----> [A, B, C]
              ↓
         客户端A 收到 ✓ → [B, C]

Postman ----enqueue----> [B, C]
              ↓
         客户端B 收到 ✓ → [C]

Postman ----enqueue----> [C]
              ↓
         客户端C 收到 ✓ → []
```

---

## ⚙️ 高级测试

### 测试1：超时机制

```bash
# 请求A接口，但不发送数据
curl http://localhost:8080/event-bridge/wait/timeout-test

# 等待30秒（方式1）或60秒（方式2-4）
# 自动返回超时信息
```

### 测试2：客户端断开

```bash
# 终端1：请求A接口
curl http://localhost:8080/event-bridge/wait/123

# 按 Ctrl+C 断开

# 控制台输出：
# 客户端断开连接，清理 sink，ID=123
```

### 测试3：没有等待的A接口

```bash
# 直接发送数据，但没有A接口在等待
curl -X POST http://localhost:8080/event-bridge/send/999 \
  -H "Content-Type: text/plain" \
  -d "No one waiting"

# 返回：❌ 没有等待的 A 接口，ID=999
```

### 测试4：并发压力测试

```bash
# 使用 apache bench 测试
ab -n 100 -c 10 http://localhost:8080/event-bridge/wait-multicast

# 同时开启100个等待连接
# 然后广播一次，观察是否所有连接都收到数据
```

---

## 🐛 常见问题

### 问题1：请求立即返回，没有等待

**症状**：A接口请求后立即返回，不等待

**原因**：可能之前有缓存数据（方式4）

**解决**：
```bash
# 重启应用清除缓存
# 或使用方式1（带ID）
```

### 问题2：终端卡住，没有反应

**症状**：终端挂起，没有任何输出

**原因**：正在等待B接口发送数据（这是正常的！）

**解决**：
```bash
# 在另一个终端发送数据
curl -X POST http://localhost:8080/event-bridge/send/{id} \
  -H "Content-Type: text/plain" \
  -d "Your data"
```

### 问题3：超时时间太短

**症状**：还没来得及发送就超时了

**解决**：修改代码中的超时时间
```java
.timeout(Duration.ofSeconds(60))  // 改为60秒
```

### 问题4：找不到等待的A接口

**症状**：B接口返回"没有等待的A接口"

**原因**：
1. A接口已超时
2. A接口还没请求
3. ID不匹配（方式1）

**解决**：
```bash
# 先请求A接口，再发送B接口
# 注意ID要匹配
```

---

## 📝 学习路径

### 入门（必做）

1. ✅ **方式1** - `/wait/{id}` + `/send/{id}`
   - 最简单，最容易理解
   - 建议先完成这个

2. ✅ **方式2** - 多播
   - 理解一对多的概念
   - 观察多个终端同时收到数据

### 进阶

3. ✅ **方式3** - 队列
   - 理解FIFO概念
   - 观察排队处理过程

4. ✅ **方式4** - 重播
   - 理解缓存机制
   - 对比有缓存和无缓存的区别

---

## ✅ 自检清单

完成测试后，确认你理解了：

- [ ] A接口如何等待不返回
- [ ] B接口如何触发A接口返回
- [ ] `Mono.create()` 的作用
- [ ] `MonoSink` 的作用
- [ ] 如何通过 `sink.success(data)` 发射数据
- [ ] 四种方式的区别和适用场景
- [ ] 超时机制的作用
- [ ] 如何清理资源避免内存泄漏

---

## 🎓 记忆口诀

```
A接口等待不返回，
Mono.create来帮忙。

MonoSink存入Map，
B接口触发把数据发。

success方法发数据，
A接口立即就返回。

四种方式各不同，
按需选择最合适。

超时清理要记牢，
避免内存被泄漏。
```

---

## 🚀 开始测试

**推荐测试顺序**：

1. 方式1 - `/wait/123` + `/send/123` ⭐ 最简单
2. 方式2 - `/wait-multicast` + `/broadcast` 
3. 方式3 - `/wait-queue` + `/enqueue`
4. 方式4 - `/wait-replay` + `/replay`

**准备工作**：
- 打开至少2个终端
- 启动Spring Boot应用
- 开始测试！

祝学习愉快！🎉

