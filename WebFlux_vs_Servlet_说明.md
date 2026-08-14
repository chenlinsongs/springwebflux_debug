# Spring WebFlux 异步工作模式详解

## 一、核心概念对比

### 1.1 Servlet 容器的工作模式（传统方式）

```
客户端请求 → Tomcat线程池 → 分配线程 → 执行Controller → 阻塞等待 → 返回响应
                                              ↓
                                         [线程被占用]
                                         等待数据库/HTTP调用
```

**特点：**
- **一请求一线程**：每个请求占用一个线程，直到响应返回
- **同步阻塞**：线程在等待IO（数据库、HTTP）时被阻塞，无法处理其他请求
- **线程成本高**：线程占用内存（默认1MB栈空间），线程切换有开销

**示例代码（Servlet风格）：**
```java
@RestController
public class ServletStyleController {
    
    @Autowired
    private RestTemplate restTemplate;  // 阻塞的HTTP客户端
    
    @GetMapping("/user/{id}")
    public User getUser(@PathVariable Long id) {
        // 1. 调用用户服务（线程阻塞等待响应）
        User user = restTemplate.getForObject("http://user-service/api/users/" + id, User.class);
        
        // 2. 调用订单服务（线程继续阻塞）
        List<Order> orders = restTemplate.getForObject("http://order-service/api/orders?userId=" + id, List.class);
        
        // 3. 处理业务逻辑
        user.setOrders(orders);
        
        // 4. 返回结果
        return user;  // 整个过程线程一直被占用
    }
}
```

**问题：**
- 假设每个HTTP调用需要100ms，上面的代码需要200ms，线程一直被占用
- 如果有1000个并发请求，需要1000个线程（每个线程1MB = 1GB内存）
- 线程数量有限，超过线程池大小的请求会被拒绝或排队

---

### 1.2 Spring WebFlux 的工作模式（响应式）

```
客户端请求 → Netty EventLoop → 创建Mono/Flux → 注册回调 → 立即返回线程
                                                    ↓
                                            [线程可以处理其他请求]
                                                    ↓
                                            IO完成时触发回调
                                                    ↓
                                            写回响应
```

**特点：**
- **异步非阻塞**：不等待IO完成，立即返回线程
- **事件驱动**：基于Netty的EventLoop机制
- **少量线程**：默认线程数 = CPU核心数 × 2
- **高吞吐量**：同样的硬件可以处理更多请求

**示例代码（WebFlux风格）：**
```java
@RestController
public class WebFluxStyleController {
    
    @Autowired
    private WebClient webClient;  // 非阻塞的HTTP客户端
    
    @GetMapping("/user/{id}")
    public Mono<User> getUser(@PathVariable Long id) {
        // 创建异步调用链，方法立即返回
        return webClient.get()
                // 1. 调用用户服务（不阻塞，返回Mono<User>）
                .uri("http://user-service/api/users/" + id)
                .retrieve()
                .bodyToMono(User.class)
                
                // 2. 用户数据到达后，调用订单服务（仍然不阻塞）
                .flatMap(user -> {
                    return webClient.get()
                            .uri("http://order-service/api/orders?userId=" + id)
                            .retrieve()
                            .bodyToFlux(Order.class)
                            .collectList()
                            .map(orders -> {
                                // 3. 组合数据
                                user.setOrders(orders);
                                return user;
                            });
                });
        
        // 这个方法会立即返回Mono，不会阻塞线程！
        // WebFlux框架会自动订阅这个Mono，数据准备好时自动写回响应
    }
}
```

**优势：**
- 两个HTTP调用可以在不同的EventLoop周期完成，线程从不阻塞
- 1000个并发请求只需要少量线程（如8-16个）
- 更高的吞吐量和更低的资源消耗

---

## 二、问题1：如何写回数据？

### 2.1 Servlet方式

```java
@GetMapping("/hello")
public String hello() {
    // 方法执行完，return的值会被写入HTTP响应
    return "Hello";  
}
// 特点：同步执行，线程阻塞等待方法返回
```

### 2.2 WebFlux方式

```java
@GetMapping("/hello")
public Mono<String> hello() {
    // 返回一个"承诺"（Promise），表示"将来会有一个String"
    return Mono.just("Hello");
}
// 特点：方法立即返回，WebFlux框架会自动订阅Mono，数据准备好时自动写回响应
```

### 2.3 为什么这样做？

**核心原因：不阻塞线程**

```
Servlet模式：
┌──────────┐
│ 请求到达  │
└─────┬────┘
      ↓ 占用线程
┌──────────┐
│ 执行业务  │ ← 线程被占用
└─────┬────┘
      ↓ 继续占用
┌──────────┐
│ 返回响应  │ ← 线程一直被占用
└─────┬────┘
      ↓ 释放线程
      
      
WebFlux模式：
┌──────────┐
│ 请求到达  │
└─────┬────┘
      ↓ 快速处理
┌──────────┐
│ 创建Mono  │
└─────┬────┘
      ↓ 立即释放线程
   [线程空闲，可以处理其他请求]
      ↓
   [数据准备好]
      ↓
   [触发回调]
      ↓
┌──────────┐
│ 写回响应  │
└──────────┘
```

---

## 三、问题2：如何调用其他服务？

### 3.1 Servlet方式

```java
@RestController
public class ServletExample {
    
    @Autowired
    private RestTemplate restTemplate;
    
    @GetMapping("/process")
    public Result processData() {
        // 步骤1：调用服务A（阻塞300ms）
        String dataA = restTemplate.getForObject("http://service-a/api", String.class);
        
        // 步骤2：调用服务B（阻塞200ms）
        String dataB = restTemplate.getForObject("http://service-b/api", String.class);
        
        // 步骤3：处理业务逻辑
        String processed = process(dataA, dataB);
        
        // 步骤4：调用服务C保存（阻塞100ms）
        String result = restTemplate.postForObject("http://service-c/api", processed, String.class);
        
        return new Result(result);
        
        // 总耗时：300 + 200 + 100 = 600ms
        // 线程占用时间：600ms
    }
}
```

### 3.2 WebFlux方式

```java
@RestController
public class WebFluxExample {
    
    @Autowired
    private WebClient webClient;
    
    @GetMapping("/process")
    public Mono<Result> processData() {
        // 创建异步调用链
        return webClient.get()
                // 步骤1：调用服务A（不阻塞）
                .uri("http://service-a/api")
                .retrieve()
                .bodyToMono(String.class)
                
                // 步骤2：调用服务B（不阻塞）
                .flatMap(dataA -> {
                    return webClient.get()
                            .uri("http://service-b/api")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(dataB -> new Tuple(dataA, dataB));
                })
                
                // 步骤3：处理业务逻辑
                .map(tuple -> process(tuple.dataA, tuple.dataB))
                
                // 步骤4：调用服务C保存
                .flatMap(processed -> {
                    return webClient.post()
                            .uri("http://service-c/api")
                            .bodyValue(processed)
                            .retrieve()
                            .bodyToMono(String.class);
                })
                
                // 步骤5：返回结果
                .map(result -> new Result(result));
        
        // 方法立即返回，总耗时仍然是600ms（网络延迟不变）
        // 但线程占用时间：几乎为0（只有创建Mono的时间）
        // 这个线程可以立即处理其他请求！
    }
}
```

### 3.3 并行调用（WebFlux的优势）

```java
@GetMapping("/parallel")
public Mono<Result> parallelCalls() {
    // 同时发起多个调用
    Mono<String> dataA = webClient.get()
            .uri("http://service-a/api")  // 300ms
            .retrieve()
            .bodyToMono(String.class);
    
    Mono<String> dataB = webClient.get()
            .uri("http://service-b/api")  // 200ms
            .retrieve()
            .bodyToMono(String.class);
    
    Mono<String> dataC = webClient.get()
            .uri("http://service-c/api")  // 100ms
            .retrieve()
            .bodyToMono(String.class);
    
    // 并行执行，等待所有完成
    return Mono.zip(dataA, dataB, dataC)
            .map(tuple -> {
                String a = tuple.getT1();
                String b = tuple.getT2();
                String c = tuple.getT3();
                return new Result(a, b, c);
            });
    
    // Servlet方式：300 + 200 + 100 = 600ms（串行）
    // WebFlux方式：max(300, 200, 100) = 300ms（并行）！
}
```

---

## 四、关键概念

### 4.1 Mono和Flux

- **Mono<T>**：表示0个或1个元素的异步序列（类似于Future或Promise）
- **Flux<T>**：表示0到N个元素的异步序列（类似于Stream）

```java
// Mono：单个值
Mono<String> mono = Mono.just("Hello");

// Flux：多个值（流）
Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5);

// 从集合创建
Flux<String> flux2 = Flux.fromIterable(Arrays.asList("A", "B", "C"));

// 空值
Mono<String> empty = Mono.empty();
```

### 4.2 常用操作符

```java
// map：转换数据
Mono<Integer> length = Mono.just("Hello")
        .map(s -> s.length());  // 结果：5

// flatMap：异步转换（返回新的Mono/Flux）
Mono<String> result = Mono.just(1)
        .flatMap(id -> webClient.get()
                .uri("/api/users/" + id)
                .retrieve()
                .bodyToMono(String.class));

// filter：过滤
Flux<Integer> even = Flux.just(1, 2, 3, 4, 5)
        .filter(n -> n % 2 == 0);  // 结果：2, 4

// zip：组合多个Mono
Mono<String> combined = Mono.zip(mono1, mono2, mono3)
        .map(tuple -> tuple.getT1() + tuple.getT2() + tuple.getT3());

// onErrorResume：错误处理
Mono<String> safe = webClient.get()
        .uri("/api/data")
        .retrieve()
        .bodyToMono(String.class)
        .onErrorResume(e -> Mono.just("默认值"));

// timeout：设置超时
Mono<String> withTimeout = webClient.get()
        .uri("/api/slow")
        .retrieve()
        .bodyToMono(String.class)
        .timeout(Duration.ofSeconds(5));
```

### 4.3 订阅机制

**重要**：Mono和Flux是**惰性的**，只有被订阅（subscribe）时才会执行。

```java
// 创建Mono（不会执行）
Mono<String> mono = webClient.get()
        .uri("/api/data")
        .retrieve()
        .bodyToMono(String.class);

// 手动订阅（会执行）
mono.subscribe(
    data -> System.out.println("收到数据：" + data),      // onNext
    error -> System.err.println("发生错误：" + error),    // onError
    () -> System.out.println("完成")                     // onComplete
);

// 在Controller中，WebFlux框架会自动订阅返回的Mono/Flux
```

---

## 五、实战场景

### 5.1 场景：查询用户信息，同时查询用户的订单和地址

**Servlet方式（串行，慢）：**
```java
@GetMapping("/user/{id}/detail")
public UserDetail getUserDetail(@PathVariable Long id) {
    User user = restTemplate.getForObject("/api/users/" + id, User.class);           // 100ms
    List<Order> orders = restTemplate.getForObject("/api/orders?userId=" + id);       // 150ms
    Address address = restTemplate.getForObject("/api/addresses?userId=" + id);       // 120ms
    
    return new UserDetail(user, orders, address);
    // 总耗时：100 + 150 + 120 = 370ms
}
```

**WebFlux方式（并行，快）：**
```java
@GetMapping("/user/{id}/detail")
public Mono<UserDetail> getUserDetail(@PathVariable Long id) {
    Mono<User> userMono = webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class);  // 100ms
    
    Mono<List<Order>> ordersMono = webClient.get()
            .uri("/api/orders?userId=" + id)
            .retrieve()
            .bodyToFlux(Order.class)
            .collectList();  // 150ms
    
    Mono<Address> addressMono = webClient.get()
            .uri("/api/addresses?userId=" + id)
            .retrieve()
            .bodyToMono(Address.class);  // 120ms
    
    // 并行执行，等待所有完成
    return Mono.zip(userMono, ordersMono, addressMono)
            .map(tuple -> new UserDetail(tuple.getT1(), tuple.getT2(), tuple.getT3()));
    
    // 总耗时：max(100, 150, 120) = 150ms（节省60%时间！）
}
```

### 5.2 场景：流式返回数据

```java
@GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<ServerSentEvent<String>> streamData() {
    return Flux.interval(Duration.ofSeconds(1))  // 每秒发送一次
            .map(seq -> ServerSentEvent.<String>builder()
                    .id(String.valueOf(seq))
                    .event("message")
                    .data("数据 " + seq)
                    .build());
    
    // 数据会实时推送给客户端，不需要等待所有数据准备好
    // Servlet很难实现这种功能
}
```

---

## 六、注意事项

### 6.1 不要阻塞线程

```java
// ❌ 错误：在WebFlux中使用阻塞操作
@GetMapping("/wrong")
public Mono<String> wrong() {
    return Mono.fromCallable(() -> {
        Thread.sleep(1000);  // 阻塞线程，破坏了异步模型！
        return "Result";
    });
}

// ✅ 正确：使用subscribeOn切换到专门的线程池
@GetMapping("/correct")
public Mono<String> correct() {
    return Mono.fromCallable(() -> {
        Thread.sleep(1000);  // CPU密集或阻塞操作
        return "Result";
    }).subscribeOn(Schedulers.boundedElastic());  // 在独立线程池执行
}
```

### 6.2 错误处理

```java
@GetMapping("/safe")
public Mono<String> safeCall() {
    return webClient.get()
            .uri("/api/data")
            .retrieve()
            .bodyToMono(String.class)
            .timeout(Duration.ofSeconds(5))  // 超时控制
            .onErrorResume(TimeoutException.class, e -> {
                // 超时时返回默认值
                return Mono.just("默认值（超时）");
            })
            .onErrorResume(WebClientException.class, e -> {
                // 网络错误时返回默认值
                return Mono.just("默认值（网络错误）");
            })
            .doOnError(e -> {
                // 记录所有错误
                log.error("调用失败", e);
            });
}
```

### 6.3 背压（Backpressure）

WebFlux支持背压，可以控制数据流速度，防止内存溢出：

```java
@GetMapping("/backpressure")
public Flux<Integer> withBackpressure() {
    return Flux.range(1, 1000000)  // 生产100万个数据
            .onBackpressureBuffer(100)  // 缓冲区最多100个
            .delayElements(Duration.ofMillis(10));  // 控制发送速度
    
    // 消费者跟不上时，生产者会暂停，不会内存溢出
}
```

---

## 七、总结

| 特性 | Servlet | WebFlux |
|------|---------|---------|
| **线程模型** | 一请求一线程 | 少量线程处理大量请求 |
| **阻塞** | 同步阻塞 | 异步非阻塞 |
| **返回类型** | T（如String, User） | Mono<T>或Flux<T> |
| **HTTP客户端** | RestTemplate | WebClient |
| **并发处理** | 依赖线程数量 | 依赖事件循环 |
| **吞吐量** | 受线程数限制 | 可以非常高 |
| **资源消耗** | 高（每个线程~1MB） | 低 |
| **适用场景** | CPU密集、传统同步代码 | IO密集、高并发、流式数据 |
| **学习曲线** | 简单 | 较陡峭 |

**选择建议：**
- 如果你的应用主要是数据库CRUD，并发不高，用Servlet就够了
- 如果你的应用需要调用多个外部服务，并发高，或者需要流式处理，WebFlux是更好的选择
- 可以混合使用：大部分接口用Servlet，高并发接口用WebFlux

**关键要记住的：**
1. WebFlux不是让单个请求变快，而是让系统能处理更多并发请求
2. Mono和Flux是"承诺"，表示"将来会有数据"，不是立即的数据
3. 方法返回Mono/Flux后立即释放线程，数据准备好时框架会自动写回响应
4. 永远不要在WebFlux中使用阻塞操作（Thread.sleep、JDBC等）

