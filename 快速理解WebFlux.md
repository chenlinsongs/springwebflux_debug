# 5分钟快速理解 Spring WebFlux

## 一句话总结

**Servlet**：一个请求占用一个线程，线程等到请求完成才释放  
**WebFlux**：一个请求不占用线程，方法立即返回，数据准备好时自动写回

---

## 问题1：WebFlux 如何返回数据？

### Servlet 方式
```java
@GetMapping("/hello")
public String hello() {
    return "Hello";  // 线程等待方法执行完，然后返回数据
}
```
**流程**：请求 → 占用线程 → 执行方法 → 返回数据 → 释放线程

---

### WebFlux 方式
```java
@GetMapping("/hello")
public Mono<String> hello() {
    return Mono.just("Hello");  // 立即返回一个"承诺"，不占用线程
}
```
**流程**：请求 → 快速创建Mono → 立即释放线程 → 数据准备好 → 自动写回

---

## 问题2：WebFlux 如何调用其他服务？

### Servlet 方式（阻塞）
```java
@GetMapping("/user/{id}")
public User getUser(@PathVariable Long id) {
    // RestTemplate 是阻塞的，线程会等待
    User user = restTemplate.getForObject("http://service/users/" + id, User.class);
    
    // 调用另一个服务，线程继续等待
    List<Order> orders = restTemplate.getForObject("http://service/orders?userId=" + id);
    
    user.setOrders(orders);
    return user;  // 总耗时 = 等待时间1 + 等待时间2
}
```

**时间线**：
```
线程占用 ━━━━[等待用户服务]━━━━[等待订单服务]━━━━ 返回
         └─────── 200ms ───────┴───── 150ms ─────┘
         总耗时：350ms，线程占用：350ms
```

---

### WebFlux 方式（非阻塞）
```java
@GetMapping("/user/{id}")
public Mono<User> getUser(@PathVariable Long id) {
    // WebClient 是非阻塞的，不等待
    return webClient.get()
            .uri("http://service/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            // 用户数据到达后，再调用订单服务
            .flatMap(user -> {
                return webClient.get()
                        .uri("http://service/orders?userId=" + id)
                        .retrieve()
                        .bodyToFlux(Order.class)
                        .collectList()
                        .map(orders -> {
                            user.setOrders(orders);
                            return user;
                        });
            });
    // 方法立即返回，不等待！
}
```

**时间线**：
```
线程占用 ─[创建Mono]─ 立即释放
           ↓
         (线程可以处理其他请求)
           ↓
         [数据到达] → [触发回调] → [写回响应]
         总耗时：350ms，线程占用：< 1ms
```

---

## 核心优势示例：并行调用

### Servlet：串行执行
```java
String result1 = restTemplate.getForObject("http://service1/api");  // 100ms
String result2 = restTemplate.getForObject("http://service2/api");  // 100ms
String result3 = restTemplate.getForObject("http://service3/api");  // 100ms
// 总耗时：300ms
```

### WebFlux：并行执行
```java
Mono<String> mono1 = webClient.get().uri("http://service1/api").retrieve().bodyToMono(String.class);
Mono<String> mono2 = webClient.get().uri("http://service2/api").retrieve().bodyToMono(String.class);
Mono<String> mono3 = webClient.get().uri("http://service3/api").retrieve().bodyToMono(String.class);

return Mono.zip(mono1, mono2, mono3)
        .map(tuple -> combine(tuple.getT1(), tuple.getT2(), tuple.getT3()));
// 总耗时：100ms（并行执行！）
```

---

## 关键概念

### Mono<T>
表示"将来会有 0 个或 1 个 T"
```java
Mono<String> mono = Mono.just("Hello");     // 有值
Mono<String> empty = Mono.empty();          // 空值
```

### Flux<T>
表示"将来会有 0 到 N 个 T"
```java
Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5);  // 多个值
```

### 常用操作
```java
// map：转换
Mono.just("hello").map(s -> s.toUpperCase())  // → "HELLO"

// flatMap：异步转换
Mono.just(1).flatMap(id -> webClient.get().uri("/api/" + id).retrieve().bodyToMono(String.class))

// zip：组合
Mono.zip(mono1, mono2, mono3).map(tuple -> ...)

// onErrorResume：错误处理
mono.onErrorResume(e -> Mono.just("默认值"))
```

---

## 为什么这样做？

### 资源利用率对比

**Servlet（1000个并发请求）：**
```
需要：1000个线程
内存：1000 × 1MB = 1GB
结果：线程池耗尽，请求被拒绝
```

**WebFlux（1000个并发请求）：**
```
需要：8-16个线程（取决于CPU核心数）
内存：16 × 1MB = 16MB
结果：轻松处理，还有余力
```

---

## 何时使用 WebFlux？

✅ **适合**：
- 高并发场景（>1000 QPS）
- 需要调用多个外部服务
- IO 密集型应用
- 需要实时数据推送（WebSocket、SSE）

❌ **不适合**：
- CPU 密集型计算
- 使用阻塞的第三方库（如 JDBC，要用 R2DBC 替代）
- 团队不熟悉响应式编程

---

## 实战示例：查看你项目中的代码

### 你的 HelloController（第477行）
```java
@PostMapping("/live3")
public Mono<Void> live3(ServerWebExchange exchange) throws IOException {
    Mono<Void> mono = webClient
            .method(HttpMethod.POST)
            .uri("http://localhost:8082/api/log/upload")
            .headers(headers -> {
                headers.addAll(exchange.getRequest().getHeaders());
            })
            .body(BodyInserters.fromDataBuffers(exchange.getRequest().getBody()))
            .exchange()
            .flatMap(new MyFunction(exchange))
            .doOnError(throwable -> cleanup(exchange));
    return mono;  // ← 立即返回，不阻塞
}
```

**这段代码做了什么？**
1. 接收请求
2. 用 WebClient 转发请求到 `http://localhost:8082/api/log/upload`
3. 立即返回 Mono（不等待响应）
4. 响应到达时，触发 `flatMap` 中的 `MyFunction`
5. 将响应写回给客户端

**关键**：整个过程中，线程从不阻塞等待！

---

## 记住这3点

1. **返回 Mono/Flux**：告诉框架"我会给你数据，但不是现在"
2. **使用 WebClient**：调用其他服务时不阻塞线程
3. **链式调用**：用 `map`、`flatMap`、`zip` 组合异步操作

就这么简单！🚀

