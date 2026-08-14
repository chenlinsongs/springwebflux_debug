# 等待外部接口返回 Mono

## 🎯 你的问题

> "如果我要返回一个 mono，这个 mono 的值需要等待另外一个接口提供，应该如何实现？"

---

## 💡 核心答案

使用 **WebClient** 调用外部接口，直接返回它的 Mono。WebFlux 会自动等待并处理。

```java
@GetMapping("/my-api")
public Mono<User> getUser() {
    // 直接返回 WebClient 的 Mono
    // WebFlux 框架会等待外部接口响应
    return webClient.get()
            .uri("http://other-service/api/users/1")
            .retrieve()
            .bodyToMono(User.class);
}
```

**关键**：你不需要"等待"，只需要返回 Mono。框架会处理等待和异步执行。

---

## 📊 几种实现方式

### 方式1：直接返回外部接口的 Mono（最简单）

```java
@RestController
public class MyController {
    
    @Autowired
    private WebClient webClient;
    
    @GetMapping("/user/{id}")
    public Mono<User> getUser(@PathVariable Long id) {
        // 直接返回外部接口的 Mono
        return webClient.get()
                .uri("http://user-service/api/users/" + id)
                .retrieve()
                .bodyToMono(User.class);
        
        // 框架会：
        // 1. subscribe 这个 Mono
        // 2. 发起 HTTP 请求
        // 3. 等待响应到达
        // 4. 将响应写回客户端
    }
}
```

---

### 方式2：调用外部接口并处理结果

```java
@GetMapping("/user/{id}/info")
public Mono<UserInfo> getUserInfo(@PathVariable Long id) {
    return webClient.get()
            .uri("http://user-service/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            // 处理外部接口返回的数据
            .map(user -> {
                UserInfo info = new UserInfo();
                info.setName(user.getName());
                info.setEmail(user.getEmail());
                info.setStatus("active");
                return info;
            });
}
```

---

### 方式3：调用多个外部接口

```java
@GetMapping("/user/{id}/details")
public Mono<UserDetails> getUserDetails(@PathVariable Long id) {
    // 调用第1个外部接口
    return webClient.get()
            .uri("http://user-service/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            // 用第1个接口的结果调用第2个接口
            .flatMap(user -> {
                return webClient.get()
                        .uri("http://order-service/api/orders?userId=" + user.getId())
                        .retrieve()
                        .bodyToFlux(Order.class)
                        .collectList()
                        .map(orders -> new UserDetails(user, orders));
            });
}
```

---

### 方式4：并行调用多个外部接口

```java
@GetMapping("/user/{id}/all-info")
public Mono<CompleteUserInfo> getCompleteInfo(@PathVariable Long id) {
    // 同时发起3个请求
    Mono<User> userMono = webClient.get()
            .uri("http://user-service/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class);
    
    Mono<List<Order>> ordersMono = webClient.get()
            .uri("http://order-service/api/orders?userId=" + id)
            .retrieve()
            .bodyToFlux(Order.class)
            .collectList();
    
    Mono<Address> addressMono = webClient.get()
            .uri("http://address-service/api/addresses?userId=" + id)
            .retrieve()
            .bodyToMono(Address.class);
    
    // 并行执行，等待所有完成
    return Mono.zip(userMono, ordersMono, addressMono)
            .map(tuple -> {
                User user = tuple.getT1();
                List<Order> orders = tuple.getT2();
                Address address = tuple.getT3();
                return new CompleteUserInfo(user, orders, address);
            });
}
```

---

### 方式5：使用 Mono.defer（延迟执行）

```java
@GetMapping("/deferred/{id}")
public Mono<User> getDeferred(@PathVariable Long id) {
    // 延迟创建 Mono，直到被订阅
    return Mono.defer(() -> {
        System.out.println("开始调用外部接口");
        return webClient.get()
                .uri("http://user-service/api/users/" + id)
                .retrieve()
                .bodyToMono(User.class);
    });
}
```

---

### 方式6：使用 Mono.create（手动控制）

```java
@GetMapping("/manual/{id}")
public Mono<User> getManual(@PathVariable Long id) {
    return Mono.create(sink -> {
        // 手动控制何时发射数据
        webClient.get()
                .uri("http://user-service/api/users/" + id)
                .retrieve()
                .bodyToMono(User.class)
                .subscribe(
                    user -> sink.success(user),      // 成功时
                    error -> sink.error(error)       // 失败时
                );
    });
}
```

---

## 🔍 详细场景示例

### 场景1：转发请求到另一个服务

```java
@RestController
public class ProxyController {
    
    @Autowired
    private WebClient webClient;
    
    // 客户端请求你的服务
    // 你的服务请求其他服务
    // 返回其他服务的响应
    @GetMapping("/proxy/user/{id}")
    public Mono<User> proxyGetUser(@PathVariable Long id) {
        // 直接转发到其他服务
        return webClient.get()
                .uri("http://other-service/api/users/" + id)
                .retrieve()
                .bodyToMono(User.class);
        
        // 流程：
        // 客户端 → 你的服务 → 其他服务
        //       ←           ←
    }
}
```

---

### 场景2：组合多个服务的数据

```java
@GetMapping("/combined/{userId}")
public Mono<CombinedData> getCombinedData(@PathVariable Long userId) {
    // 步骤1：获取用户信息
    return webClient.get()
            .uri("http://user-service/api/users/" + userId)
            .retrieve()
            .bodyToMono(User.class)
            
            // 步骤2：用用户信息获取订单
            .flatMap(user -> {
                return webClient.get()
                        .uri("http://order-service/api/orders?userId=" + user.getId())
                        .retrieve()
                        .bodyToFlux(Order.class)
                        .collectList()
                        // 步骤3：组合结果
                        .map(orders -> new CombinedData(user, orders));
            });
}
```

---

### 场景3：带超时和错误处理

```java
@GetMapping("/safe/{id}")
public Mono<User> getSafeUser(@PathVariable Long id) {
    return webClient.get()
            .uri("http://user-service/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            // 设置超时
            .timeout(Duration.ofSeconds(5))
            // 错误时返回默认值
            .onErrorResume(TimeoutException.class, e -> {
                System.err.println("超时，返回默认用户");
                return Mono.just(new User(-1L, "默认用户"));
            })
            .onErrorResume(WebClientException.class, e -> {
                System.err.println("网络错误，返回默认用户");
                return Mono.just(new User(-1L, "默认用户"));
            });
}
```

---

### 场景4：有条件的调用

```java
@GetMapping("/conditional/{id}")
public Mono<UserData> getConditionalData(@PathVariable Long id) {
    return webClient.get()
            .uri("http://user-service/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            .flatMap(user -> {
                // 根据条件决定是否调用其他接口
                if (user.isVip()) {
                    // VIP 用户，获取额外信息
                    return webClient.get()
                            .uri("http://vip-service/api/vip-info/" + user.getId())
                            .retrieve()
                            .bodyToMono(VipInfo.class)
                            .map(vipInfo -> new UserData(user, vipInfo));
                } else {
                    // 普通用户，直接返回
                    return Mono.just(new UserData(user, null));
                }
            });
}
```

---

### 场景5：重试机制

```java
@GetMapping("/retry/{id}")
public Mono<User> getUserWithRetry(@PathVariable Long id) {
    return webClient.get()
            .uri("http://user-service/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            // 失败时重试3次
            .retry(3)
            // 或者使用退避重试
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
            // 最后仍失败，返回默认值
            .onErrorResume(e -> {
                System.err.println("重试3次后仍失败");
                return Mono.just(new User(-1L, "默认用户"));
            });
}
```

---

## 🎨 工作流程可视化

### 单个外部接口

```
客户端请求
    ↓
你的 Controller
    ↓
返回 Mono（WebClient.get()...）
    ↓
框架 subscribe
    ↓
发起 HTTP 请求到外部服务
    ↓
[等待外部服务响应...] ← 线程不阻塞
    ↓
外部服务响应到达
    ↓
触发 onNext 回调
    ↓
框架将数据写回客户端
    ↓
完成
```

### 串行调用多个接口

```
客户端请求
    ↓
Controller 返回 Mono
    ↓
框架 subscribe
    ↓
调用服务1
    ↓
[等待服务1...] ← 不阻塞
    ↓
服务1响应到达
    ↓
flatMap 调用服务2
    ↓
[等待服务2...] ← 不阻塞
    ↓
服务2响应到达
    ↓
组合结果
    ↓
写回客户端
```

### 并行调用多个接口

```
客户端请求
    ↓
Controller 返回 Mono.zip(...)
    ↓
框架 subscribe
    ↓
同时发起3个请求
    ├─→ 服务1 [200ms]
    ├─→ 服务2 [150ms]
    └─→ 服务3 [100ms]
    ↓
等待所有响应（max=200ms）
    ↓
组合结果
    ↓
写回客户端

总耗时：200ms（而不是 450ms）
```

---

## 💡 关键理解

### 1. 不需要"等待"

```java
// ❌ 错误想法："我需要等待外部接口"
@GetMapping("/user")
public Mono<User> getUser() {
    Mono<User> mono = webClient.get()...;
    
    // ❌ 不要这样做
    User user = mono.block();  // 阻塞等待
    return Mono.just(user);
}

// ✅ 正确做法：直接返回 Mono
@GetMapping("/user")
public Mono<User> getUser() {
    // 直接返回，框架会处理等待
    return webClient.get()...;
}
```

### 2. 框架会自动等待

```java
// 你写：
return webClient.get()...;

// 框架做：
Mono<User> mono = controller.getUser();
mono.subscribe(
    user -> {
        // 等待数据到达
        // 写入 HTTP 响应
        response.write(toJson(user));
    }
);
```

### 3. 异步不是"快"，是"不阻塞"

```java
// 调用外部接口耗时 200ms
return webClient.get()...;

// 响应时间仍然是 200ms（不会更快）
// 但线程不会阻塞 200ms
// 线程可以处理其他请求
```

---

## ⚠️ 常见错误

### 错误1：使用 block() 等待

```java
// ❌ 错误：破坏了异步模型
@GetMapping("/wrong")
public Mono<User> wrong() {
    Mono<User> mono = webClient.get()
            .uri("http://other-service/api/users/1")
            .retrieve()
            .bodyToMono(User.class);
    
    // ❌ block() 会阻塞线程
    User user = mono.block();
    
    return Mono.just(user);
}

// ✅ 正确：直接返回 Mono
@GetMapping("/correct")
public Mono<User> correct() {
    return webClient.get()
            .uri("http://other-service/api/users/1")
            .retrieve()
            .bodyToMono(User.class);
}
```

### 错误2：在 Mono 外部调用接口

```java
// ❌ 错误：在 subscribe 前调用接口
@GetMapping("/wrong2")
public Mono<User> wrong2() {
    // ❌ 这里会立即阻塞（如果用 RestTemplate）
    User user = restTemplate.getForObject("...", User.class);
    return Mono.just(user);
}

// ✅ 正确：在 Mono 内部调用
@GetMapping("/correct2")
public Mono<User> correct2() {
    return webClient.get()
            .uri("...")
            .retrieve()
            .bodyToMono(User.class);
}
```

### 错误3：混用 RestTemplate 和 WebFlux

```java
// ❌ 错误：在 WebFlux 中使用 RestTemplate
@GetMapping("/wrong3")
public Mono<User> wrong3() {
    return Mono.fromCallable(() -> {
        // ❌ RestTemplate 是阻塞的
        return restTemplate.getForObject("...", User.class);
    });
}

// ✅ 正确：使用 WebClient
@GetMapping("/correct3")
public Mono<User> correct3() {
    return webClient.get()
            .uri("...")
            .retrieve()
            .bodyToMono(User.class);
}
```

---

## 📝 最佳实践

### 1. 配置 WebClient Bean

```java
@Configuration
public class WebClientConfig {
    
    @Bean
    public WebClient webClient(WebClient.Builder builder) {
        return builder
                // 设置基础 URL
                .baseUrl("http://other-service")
                // 设置超时
                .clientConnector(new ReactorClientHttpConnector(
                    HttpClient.create()
                        .responseTimeout(Duration.ofSeconds(5))
                ))
                // 设置默认 header
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}
```

### 2. 统一错误处理

```java
@GetMapping("/user/{id}")
public Mono<User> getUser(@PathVariable Long id) {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            // 统一错误处理
            .onErrorResume(this::handleError);
}

private Mono<User> handleError(Throwable error) {
    if (error instanceof TimeoutException) {
        return Mono.error(new ServiceException("服务超时"));
    } else if (error instanceof WebClientResponseException) {
        WebClientResponseException e = (WebClientResponseException) error;
        if (e.getStatusCode().is4xxClientError()) {
            return Mono.error(new NotFoundException("用户不存在"));
        }
    }
    return Mono.error(new ServiceException("服务异常"));
}
```

### 3. 使用缓存避免重复调用

```java
@GetMapping("/cached/{id}")
public Mono<User> getCachedUser(@PathVariable Long id) {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            .cache(Duration.ofMinutes(5));  // 缓存5分钟
}
```

---

## ✅ 总结

### 核心要点

1. **直接返回 WebClient 的 Mono**
   ```java
   return webClient.get()...
   ```

2. **不需要手动等待**
   ```
   框架会自动 subscribe 并等待
   ```

3. **使用 flatMap 链接多个调用**
   ```java
   return call1()
       .flatMap(result1 -> call2(result1))
       .flatMap(result2 -> call3(result2))
   ```

4. **使用 Mono.zip 并行调用**
   ```java
   return Mono.zip(call1(), call2(), call3())
       .map(tuple -> combine(...))
   ```

5. **不要使用 block()**
   ```
   block() 会破坏异步模型
   ```

---

## 🎯 记忆口诀

```
外部接口返回 Mono，
直接返回不用等。

框架自动会 subscribe，
异步等待它负责。

多个接口要串行，
flatMap 来帮忙。

多个接口要并行，
Mono.zip 最合适。

千万不要用 block，
阻塞线程是大忌。
```

---

现在你知道了：**直接返回 WebClient 的 Mono，框架会自动等待外部接口响应！** 🎯

