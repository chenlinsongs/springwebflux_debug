# subscribe 方法详解

## 🎯 你的问题

> "flux 有多个 subscribe 方法，比如有的参数是 Consumer，有的参数是 Subscriber，为什么 subscribe 会有这么多不同参数的方法？"

---

## 💡 核心答案

**为了方便和灵活性！**

Reactor 提供了多个 subscribe 重载版本，从最简单到最完整：
- 简单场景：只需要数据 → 用 `Consumer`
- 完整场景：需要完整控制 → 用 `Subscriber`

---

## 📊 subscribe 的所有重载版本

### Mono 和 Flux 都有以下重载

```java
// 1. 无参数版本（最简单）
Disposable subscribe()

// 2. 只处理数据（Consumer）
Disposable subscribe(Consumer<? super T> consumer)

// 3. 处理数据 + 错误
Disposable subscribe(
    Consumer<? super T> consumer,
    Consumer<? super Throwable> errorConsumer
)

// 4. 处理数据 + 错误 + 完成
Disposable subscribe(
    Consumer<? super T> consumer,
    Consumer<? super Throwable> errorConsumer,
    Runnable completeConsumer
)

// 5. 处理数据 + 错误 + 完成 + 订阅
Disposable subscribe(
    Consumer<? super T> consumer,
    Consumer<? super Throwable> errorConsumer,
    Runnable completeConsumer,
    Consumer<? super Subscription> subscriptionConsumer
)

// 6. 完整版本（Subscriber）
void subscribe(Subscriber<? super T> actual)
```

---

## 🔍 详细解释每个版本

### 版本1：无参数 subscribe()

```java
// 最简单的版本
Mono<String> mono = Mono.just("Hello");
mono.subscribe();

// 用途：
// - 只是触发执行，不关心结果
// - 忽略所有数据、错误和完成信号
// - 相当于"启动"

// 示例场景：
// - 异步执行某个操作，但不需要结果
// - 触发副作用（如记录日志、发送通知）
Mono.fromRunnable(() -> System.out.println("执行任务"))
    .subscribe();  // 只是触发执行
```

**返回值**：`Disposable` - 可以用来取消订阅

---

### 版本2：subscribe(Consumer)

```java
// 只处理数据
Mono<String> mono = Mono.just("Hello");
mono.subscribe(data -> {
    System.out.println("收到数据：" + data);
});

// 用途：
// - 只关心数据
// - 不关心错误和完成
// - 最常用的版本

// 等价于：
mono.subscribe(new Subscriber<String>() {
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);  // 请求所有数据
    }
    
    public void onNext(String data) {
        System.out.println("收到数据：" + data);  // 你的 Consumer
    }
    
    public void onError(Throwable t) {
        // 默认：打印错误并传播
    }
    
    public void onComplete() {
        // 默认：什么都不做
    }
});
```

**优点**：简洁，适合大多数场景

**缺点**：如果发生错误，会打印到控制台，但你无法自定义处理

---

### 版本3：subscribe(Consumer, Consumer)

```java
// 处理数据 + 错误
Mono<String> mono = webClient.get()
        .uri("/api/data")
        .retrieve()
        .bodyToMono(String.class);

mono.subscribe(
    data -> {
        // onNext: 处理数据
        System.out.println("成功：" + data);
    },
    error -> {
        // onError: 处理错误
        System.err.println("失败：" + error.getMessage());
    }
);

// 用途：
// - 需要处理错误
// - 不关心完成信号
// - 适合 HTTP 调用、数据库查询等可能失败的操作
```

**优点**：可以自定义错误处理

**缺点**：仍然不知道流何时完成

---

### 版本4：subscribe(Consumer, Consumer, Runnable)

```java
// 处理数据 + 错误 + 完成
Flux<Integer> flux = Flux.range(1, 5);

flux.subscribe(
    data -> {
        // onNext: 处理每个数据
        System.out.println("数据：" + data);
    },
    error -> {
        // onError: 处理错误
        System.err.println("错误：" + error.getMessage());
    },
    () -> {
        // onComplete: 完成时执行
        System.out.println("完成！");
    }
);

// 输出：
// 数据：1
// 数据：2
// 数据：3
// 数据：4
// 数据：5
// 完成！

// 用途：
// - 需要知道流何时完成
// - 完成时执行清理、统计等操作
// - 适合处理多个数据的流
```

**优点**：完整的生命周期管理

**缺点**：没有背压控制

---

### 版本5：subscribe(Consumer, Consumer, Runnable, Consumer)

```java
// 处理数据 + 错误 + 完成 + 订阅
Flux<Integer> flux = Flux.range(1, 100);

flux.subscribe(
    data -> {
        // onNext: 处理数据
        System.out.println("数据：" + data);
    },
    error -> {
        // onError: 处理错误
        System.err.println("错误：" + error.getMessage());
    },
    () -> {
        // onComplete: 完成
        System.out.println("完成！");
    },
    subscription -> {
        // onSubscribe: 订阅时执行
        // 可以控制请求数量（背压）
        subscription.request(10);  // 只请求10个数据
    }
);

// 用途：
// - 需要背压控制
// - 控制数据流速度
// - 防止内存溢出
```

**优点**：完全控制，包括背压

**缺点**：代码冗长

---

### 版本6：subscribe(Subscriber)

```java
// 完整版本：使用 Subscriber 接口
Mono<String> mono = Mono.just("Hello");

mono.subscribe(new Subscriber<String>() {
    private Subscription subscription;
    
    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        System.out.println("订阅成功");
        s.request(1);  // 请求1个数据
    }
    
    @Override
    public void onNext(String data) {
        System.out.println("收到数据：" + data);
    }
    
    @Override
    public void onError(Throwable t) {
        System.err.println("发生错误：" + t.getMessage());
    }
    
    @Override
    public void onComplete() {
        System.out.println("完成");
    }
});

// 用途：
// - 需要完全控制
// - 实现自定义的订阅逻辑
// - 框架内部使用
// - 高级场景（如自定义背压策略）
```

**优点**：完全控制，最灵活

**缺点**：代码最冗长，复杂

---

## 🎨 可视化对比

```
┌─────────────────────────────────────────────────────────┐
│            subscribe 方法对比                            │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  简单 ←──────────────────────────────────→ 复杂          │
│                                                          │
│  1. subscribe()                                          │
│     └─ 触发执行，不处理任何信号                          │
│                                                          │
│  2. subscribe(Consumer<T>)                               │
│     └─ 处理数据（onNext）                                │
│                                                          │
│  3. subscribe(Consumer<T>, Consumer<Throwable>)          │
│     └─ 处理数据 + 错误                                   │
│                                                          │
│  4. subscribe(Consumer, Consumer, Runnable)              │
│     └─ 处理数据 + 错误 + 完成                            │
│                                                          │
│  5. subscribe(Consumer, Consumer, Runnable, Consumer)    │
│     └─ 处理数据 + 错误 + 完成 + 订阅（背压）             │
│                                                          │
│  6. subscribe(Subscriber<T>)                             │
│     └─ 完全自定义，最灵活                                │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## 💡 为什么设计这么多版本？

### 1. 便利性原则

```java
// 如果只有 Subscriber 版本，简单场景也需要写很多代码
mono.subscribe(new Subscriber<String>() {
    public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
    public void onNext(String data) { System.out.println(data); }
    public void onError(Throwable t) { }
    public void onComplete() { }
});

// 有了 Consumer 版本，简单场景只需要一行
mono.subscribe(data -> System.out.println(data));
```

### 2. 渐进式复杂度

```
场景1：只要数据
  → subscribe(Consumer)

场景2：需要处理错误
  → subscribe(Consumer, Consumer)

场景3：需要知道完成
  → subscribe(Consumer, Consumer, Runnable)

场景4：需要背压控制
  → subscribe(Consumer, Consumer, Runnable, Consumer)

场景5：需要完全自定义
  → subscribe(Subscriber)
```

### 3. 符合 Reactive Streams 规范

```java
// Reactive Streams 规范要求实现 Subscriber 接口
public interface Subscriber<T> {
    void onSubscribe(Subscription s);
    void onNext(T t);
    void onError(Throwable t);
    void onComplete();
}

// Reactor 提供了便利方法（Consumer 版本）
// 但底层仍然使用 Subscriber
```

---

## 📝 使用建议

### 场景1：只需要数据

```java
// ✅ 推荐：使用 Consumer 版本
userRepository.findById(1)
    .subscribe(user -> System.out.println(user.getName()));
```

### 场景2：需要处理错误

```java
// ✅ 推荐：使用两个 Consumer
webClient.get()
    .uri("/api/data")
    .retrieve()
    .bodyToMono(String.class)
    .subscribe(
        data -> System.out.println("成功：" + data),
        error -> System.err.println("失败：" + error.getMessage())
    );
```

### 场景3：需要知道完成

```java
// ✅ 推荐：使用三个参数版本
Flux.range(1, 10)
    .subscribe(
        data -> System.out.println("数据：" + data),
        error -> System.err.println("错误：" + error),
        () -> System.out.println("完成！")
    );
```

### 场景4：需要背压控制

```java
// ✅ 推荐：使用四个参数版本
Flux.range(1, 1000000)
    .subscribe(
        data -> process(data),
        error -> handleError(error),
        () -> cleanup(),
        subscription -> subscription.request(100)  // 一次只请求100个
    );
```

### 场景5：需要完全自定义

```java
// ✅ 推荐：使用 Subscriber 接口
mono.subscribe(new BaseSubscriber<String>() {
    @Override
    protected void hookOnSubscribe(Subscription subscription) {
        // 自定义订阅逻辑
        request(1);
    }
    
    @Override
    protected void hookOnNext(String value) {
        // 自定义数据处理
        if (shouldContinue(value)) {
            request(1);  // 动态请求
        }
    }
});
```

---

## 🔬 内部实现

### Consumer 版本如何转换为 Subscriber？

```java
// Reactor 内部实现（简化版）
public Disposable subscribe(Consumer<? super T> consumer) {
    // 将 Consumer 包装成 Subscriber
    return subscribe(new LambdaSubscriber<>(
        consumer,           // onNext
        null,              // onError (默认)
        null,              // onComplete (默认)
        null               // onSubscribe (默认)
    ));
}

// LambdaSubscriber 实现
class LambdaSubscriber<T> implements Subscriber<T> {
    private final Consumer<? super T> consumer;
    private final Consumer<? super Throwable> errorConsumer;
    private final Runnable completeConsumer;
    
    @Override
    public void onSubscribe(Subscription s) {
        s.request(Long.MAX_VALUE);  // 默认请求所有
    }
    
    @Override
    public void onNext(T t) {
        if (consumer != null) {
            consumer.accept(t);  // 调用你的 Consumer
        }
    }
    
    @Override
    public void onError(Throwable t) {
        if (errorConsumer != null) {
            errorConsumer.accept(t);
        } else {
            // 默认错误处理
            Operators.onErrorDropped(t, Context.empty());
        }
    }
    
    @Override
    public void onComplete() {
        if (completeConsumer != null) {
            completeConsumer.run();
        }
    }
}
```

---

## 📋 对比表格

| 版本 | 参数 | 用途 | 优点 | 缺点 |
|------|------|------|------|------|
| `subscribe()` | 无 | 触发执行 | 最简单 | 忽略所有信号 |
| `subscribe(Consumer)` | 1个 | 处理数据 | 简洁 | 无法自定义错误处理 |
| `subscribe(Consumer, Consumer)` | 2个 | 数据+错误 | 可处理错误 | 不知道完成 |
| `subscribe(Consumer, Consumer, Runnable)` | 3个 | 数据+错误+完成 | 完整生命周期 | 无背压控制 |
| `subscribe(..., Consumer)` | 4个 | 全部+背压 | 有背压控制 | 代码冗长 |
| `subscribe(Subscriber)` | Subscriber | 完全自定义 | 最灵活 | 最复杂 |

---

## 🎯 常见场景选择

```java
// 场景1：打印日志
mono.subscribe(data -> log.info("收到：{}", data));
// 使用：subscribe(Consumer)

// 场景2：HTTP 调用（可能失败）
webClient.get()...
    .subscribe(
        data -> handle(data),
        error -> handleError(error)
    );
// 使用：subscribe(Consumer, Consumer)

// 场景3：处理流（需要知道完成）
flux.subscribe(
    data -> process(data),
    error -> log.error("错误", error),
    () -> log.info("完成")
);
// 使用：subscribe(Consumer, Consumer, Runnable)

// 场景4：大量数据（需要背压）
flux.subscribe(
    data -> process(data),
    error -> log.error("错误", error),
    () -> log.info("完成"),
    sub -> sub.request(100)
);
// 使用：subscribe(..., Consumer)

// 场景5：自定义订阅逻辑
flux.subscribe(new CustomSubscriber());
// 使用：subscribe(Subscriber)
```

---

## ✅ 总结

### 核心要点

1. **多个重载是为了便利性**
   ```
   简单场景 → 简单方法（Consumer）
   复杂场景 → 复杂方法（Subscriber）
   ```

2. **选择原则：够用就好**
   ```
   只要数据 → Consumer
   要处理错误 → 两个 Consumer
   要知道完成 → 三个参数
   要背压控制 → 四个参数
   要完全自定义 → Subscriber
   ```

3. **底层都是 Subscriber**
   ```java
   // Consumer 版本只是语法糖
   // 最终都会转换为 Subscriber
   subscribe(Consumer) → subscribe(new LambdaSubscriber(...))
   ```

4. **WebFlux 中通常用 Consumer 版本**
   ```java
   // Controller 返回 Mono，框架会 subscribe
   // 你很少需要手动 subscribe
   // 如果需要，通常 Consumer 版本就够了
   ```

---

## 🎓 记忆口诀

```
subscribe 重载多，
从简到繁有层次。

只要数据用 Consumer，
错误处理加一个。

完成信号要知道，
三个参数不能少。

背压控制需要时，
四个参数全用上。

完全自定义场景，
Subscriber 来帮忙。
```

---

## 📚 延伸阅读

- **[WebFlux自动订阅机制.md](./WebFlux自动订阅机制.md)** - subscribe 的作用
- **[多次订阅问题详解.md](./多次订阅问题详解.md)** - subscribe 的注意事项

---

现在你理解了：**多个 subscribe 重载是为了便利性，从简单到复杂，满足不同场景！** 🎯

