# flatMap 详解：WebFlux vs Java Stream

## 核心问题

### 问题1：flatMap 有什么作用？
**答案**：`flatMap` 用于**异步转换**，它可以将一个值转换为另一个异步操作（Mono/Flux）

### 问题2：WebFlux 的 flatMap 和 Java Stream 的 flatMap 有什么区别？
**答案**：
- **Java Stream 的 flatMap**：同步操作，用于扁平化嵌套集合
- **WebFlux 的 flatMap**：异步操作，用于链式调用异步操作

---

## 一、Java Stream 的 flatMap（同步）

### 1.1 基本概念

Java Stream 的 `flatMap` 用于**扁平化嵌套集合**：

```java
// 没有 flatMap：嵌套的 Stream
List<List<Integer>> nested = Arrays.asList(
    Arrays.asList(1, 2, 3),
    Arrays.asList(4, 5, 6),
    Arrays.asList(7, 8, 9)
);

Stream<List<Integer>> stream = nested.stream();
// 结果：Stream<List<Integer>>，还是嵌套的

// 使用 flatMap：扁平化
Stream<Integer> flattened = nested.stream()
        .flatMap(list -> list.stream());
// 结果：Stream<Integer>，变成扁平的了
// 输出：1, 2, 3, 4, 5, 6, 7, 8, 9
```

### 1.2 典型示例

```java
// 示例1：将多个用户的订单扁平化
List<User> users = Arrays.asList(user1, user2, user3);

// 每个用户有多个订单
List<Order> allOrders = users.stream()
        .flatMap(user -> user.getOrders().stream())  // List<Order> → Stream<Order>
        .collect(Collectors.toList());

// 示例2：拆分字符串
List<String> sentences = Arrays.asList("Hello World", "Spring Boot");

List<String> words = sentences.stream()
        .flatMap(sentence -> Arrays.stream(sentence.split(" ")))  // String → Stream<String>
        .collect(Collectors.toList());
// 结果：["Hello", "World", "Spring", "Boot"]
```

### 1.3 类型签名

```java
// Java Stream
<R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper)

// 解释：
// T        → 输入类型（原始元素）
// Stream<R> → 输出类型（扁平化后的流）
// mapper   → 将 T 转换为 Stream<R> 的函数
```

### 1.4 工作原理（同步）

```
输入：Stream<List<Integer>>

  ┌─────────┐
  │ [1,2,3] │ ─→ flatMap ─→ 1, 2, 3
  ├─────────┤
  │ [4,5,6] │ ─→ flatMap ─→ 4, 5, 6
  ├─────────┤
  │ [7,8,9] │ ─→ flatMap ─→ 7, 8, 9
  └─────────┘

输出：Stream<Integer> = 1, 2, 3, 4, 5, 6, 7, 8, 9

特点：同步、立即执行、用于扁平化
```

---

## 二、WebFlux 的 flatMap（异步）

### 2.1 基本概念

WebFlux 的 `flatMap` 用于**链式调用异步操作**：

```java
// 问题：如果使用 map 会怎样？
Mono<Integer> mono = Mono.just(1);

Mono<Mono<String>> nested = mono.map(id -> {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(String.class);  // 返回 Mono<String>
});
// 结果：Mono<Mono<String>>，嵌套了！无法直接使用

// 使用 flatMap：扁平化 + 异步
Mono<String> flattened = mono.flatMap(id -> {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(String.class);  // 返回 Mono<String>
});
// 结果：Mono<String>，扁平的，可以直接使用
```

### 2.2 典型示例

```java
// 示例1：根据用户ID获取用户，再获取用户的订单
@GetMapping("/user/{id}/orders")
public Mono<UserWithOrders> getUserOrders(@PathVariable Long id) {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class)
            
            // ✅ 使用 flatMap：用户数据到达后，再请求订单
            .flatMap(user -> {
                return webClient.get()
                        .uri("/api/orders?userId=" + user.getId())
                        .retrieve()
                        .bodyToFlux(Order.class)
                        .collectList()
                        .map(orders -> new UserWithOrders(user, orders));
            });
    
    // 如果用 map 会怎样？
    // .map(user -> {
    //     return webClient.get()...  // 返回 Mono<UserWithOrders>
    // })
    // 结果：Mono<Mono<UserWithOrders>>，嵌套了！
}

// 示例2：连续调用3个服务
public Mono<Result> processOrder(Long orderId) {
    return webClient.get()
            .uri("/api/orders/" + orderId)
            .retrieve()
            .bodyToMono(Order.class)
            
            // 第1次 flatMap：获取订单后，查询用户信息
            .flatMap(order -> {
                return webClient.get()
                        .uri("/api/users/" + order.getUserId())
                        .retrieve()
                        .bodyToMono(User.class)
                        .map(user -> new OrderWithUser(order, user));
            })
            
            // 第2次 flatMap：获取用户后，查询支付信息
            .flatMap(orderWithUser -> {
                return webClient.get()
                        .uri("/api/payments/" + orderWithUser.getOrder().getId())
                        .retrieve()
                        .bodyToMono(Payment.class)
                        .map(payment -> new Result(orderWithUser, payment));
            });
}
```

### 2.3 类型签名

```java
// WebFlux Mono
<R> Mono<R> flatMap(Function<? super T, ? extends Mono<? extends R>> transformer)

// 解释：
// T       → 输入类型（上游 Mono 的值）
// Mono<R> → 输出类型（下游 Mono）
// transformer → 将 T 转换为 Mono<R> 的异步函数
```

### 2.4 工作原理（异步）

```
时间轴 →

输入：Mono<Integer> = 1

  Mono.just(1)
       ↓
  flatMap(id -> webClient.get("/users/" + id))
       ↓
  [发起HTTP请求]  ← 不阻塞，立即返回
       ↓
  [等待响应...]  ← 线程可以处理其他请求
       ↓
  [响应到达]
       ↓
  返回 Mono<User>

输出：Mono<User>

特点：异步、非阻塞、用于链式调用
```

---

## 三、对比：map vs flatMap

### 3.1 何时使用 map？

当转换函数**返回普通值**时，使用 `map`：

```java
// ✅ 使用 map：返回普通值
Mono<Integer> mono = Mono.just("hello");

Mono<Integer> length = mono.map(s -> s.length());  // String → int
// 结果：Mono<Integer>

Mono<String> upper = mono.map(s -> s.toUpperCase());  // String → String
// 结果：Mono<String>
```

### 3.2 何时使用 flatMap？

当转换函数**返回 Mono/Flux**时，使用 `flatMap`：

```java
// ✅ 使用 flatMap：返回 Mono/Flux
Mono<Integer> mono = Mono.just(1);

Mono<User> user = mono.flatMap(id -> {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class);  // 返回 Mono<User>
});
// 结果：Mono<User>（扁平的）

// ❌ 如果用 map 会怎样？
Mono<Mono<User>> nested = mono.map(id -> {
    return webClient.get()...  // 返回 Mono<User>
});
// 结果：Mono<Mono<User>>（嵌套的，无法使用）
```

### 3.3 对比表格

| 特性 | map | flatMap |
|------|-----|---------|
| 转换函数返回 | **普通值** T → R | **Mono/Flux** T → Mono\<R> |
| 结果类型 | Mono\<R> | Mono\<R> |
| 用途 | 同步转换 | 异步转换（链式调用） |
| 是否阻塞 | 否（但函数内不应阻塞） | 否 |
| 典型场景 | 数据转换、计算 | 调用其他服务、数据库查询 |

---

## 四、完整对比：Java Stream vs WebFlux

### 4.1 相同点

都叫 `flatMap`，都用于"扁平化"

### 4.2 不同点

| 特性 | Java Stream flatMap | WebFlux flatMap |
|------|---------------------|-----------------|
| **执行模型** | 同步、立即执行 | 异步、延迟执行 |
| **输入类型** | Stream\<T> | Mono\<T> 或 Flux\<T> |
| **转换函数返回** | Stream\<R> | Mono\<R> 或 Flux\<R> |
| **主要用途** | 扁平化嵌套集合 | 链式调用异步操作 |
| **阻塞** | 同步执行（会占用线程） | 非阻塞（不占用线程） |
| **典型场景** | 拆分、展开集合 | HTTP调用、数据库查询 |

---

## 五、实战示例对比

### 5.1 Java Stream 示例（同步）

```java
// 场景：获取所有用户的所有订单
public List<Order> getAllOrders(List<User> users) {
    return users.stream()
            .flatMap(user -> {
                // 同步调用，阻塞线程
                List<Order> orders = orderService.getOrdersByUserId(user.getId());
                return orders.stream();
            })
            .collect(Collectors.toList());
    
    // 如果有3个用户，每个用户的订单查询需要100ms
    // 总耗时：300ms（串行执行，阻塞线程）
}
```

### 5.2 WebFlux 示例（异步）

```java
// 场景：获取所有用户的所有订单
public Flux<Order> getAllOrders(Flux<User> users) {
    return users
            .flatMap(user -> {
                // 异步调用，不阻塞线程
                return webClient.get()
                        .uri("/api/orders?userId=" + user.getId())
                        .retrieve()
                        .bodyToFlux(Order.class);
            });
    
    // 如果有3个用户，每个用户的订单查询需要100ms
    // 总耗时：约100ms（并发执行，不阻塞线程）
}
```

---

## 六、常见错误和解决方案

### 6.1 错误1：在 flatMap 中使用 map

```java
// ❌ 错误：应该用 flatMap 却用了 map
Mono<Integer> userId = Mono.just(1);

Mono<Mono<User>> wrong = userId.map(id -> {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class);  // 返回 Mono<User>
});
// 结果：Mono<Mono<User>>，嵌套了！

// ✅ 正确：使用 flatMap
Mono<User> correct = userId.flatMap(id -> {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class);  // 返回 Mono<User>
});
// 结果：Mono<User>，扁平的
```

### 6.2 错误2：在 map 中使用 flatMap

```java
// ❌ 错误：应该用 map 却用了 flatMap
Mono<String> name = Mono.just("hello");

Mono<String> wrong = name.flatMap(s -> {
    return Mono.just(s.toUpperCase());  // 没必要包装成 Mono
});

// ✅ 正确：使用 map
Mono<String> correct = name.map(s -> s.toUpperCase());
```

### 6.3 错误3：在 flatMap 中阻塞

```java
// ❌ 错误：在 flatMap 中阻塞线程
Mono<User> wrong = Mono.just(1)
        .flatMap(id -> {
            // 使用 RestTemplate（阻塞）
            User user = restTemplate.getForObject("/api/users/" + id, User.class);
            return Mono.just(user);
        });
// 问题：破坏了异步模型，EventLoop线程被阻塞

// ✅ 正确：使用非阻塞的 WebClient
Mono<User> correct = Mono.just(1)
        .flatMap(id -> {
            return webClient.get()
                    .uri("/api/users/" + id)
                    .retrieve()
                    .bodyToMono(User.class);
        });
```

---

## 七、高级用法

### 7.1 嵌套 flatMap

```java
// 场景：订单 → 用户 → 地址
public Mono<OrderWithDetails> getOrderDetails(Long orderId) {
    return webClient.get()
            .uri("/api/orders/" + orderId)
            .retrieve()
            .bodyToMono(Order.class)
            
            // 第1层 flatMap：获取用户
            .flatMap(order -> {
                return webClient.get()
                        .uri("/api/users/" + order.getUserId())
                        .retrieve()
                        .bodyToMono(User.class)
                        
                        // 第2层 flatMap：获取地址
                        .flatMap(user -> {
                            return webClient.get()
                                    .uri("/api/addresses/" + user.getAddressId())
                                    .retrieve()
                                    .bodyToMono(Address.class)
                                    .map(address -> {
                                        return new OrderWithDetails(order, user, address);
                                    });
                        });
            });
}
```

### 7.2 flatMapMany（Mono → Flux）

```java
// 将 Mono 转换为 Flux
Mono<User> userMono = Mono.just(user);

Flux<Order> orders = userMono.flatMapMany(user -> {
    return webClient.get()
            .uri("/api/orders?userId=" + user.getId())
            .retrieve()
            .bodyToFlux(Order.class);  // 返回 Flux
});
// Mono<User> → Flux<Order>
```

### 7.3 flatMapIterable（Mono → Flux）

```java
// 将 Mono 中的集合展开成 Flux
Mono<List<String>> listMono = Mono.just(Arrays.asList("a", "b", "c"));

Flux<String> flux = listMono.flatMapIterable(list -> list);
// Mono<List<String>> → Flux<String>
```

---

## 八、记忆技巧

### 8.1 简单记忆法

```
map:     T  →  R         (同步转换，返回普通值)
flatMap: T  →  Mono<R>   (异步转换，返回 Mono/Flux)
```

### 8.2 决策树

```
我的转换函数返回什么？
├─ 返回普通值（int, String, User等）
│  └─ 使用 map
│
└─ 返回 Mono/Flux（异步操作）
   └─ 使用 flatMap
```

### 8.3 口诀

```
同步转换用 map，
异步调用 flatMap。
返回普通用 map，
返回 Mono 用 flatMap。
```

---

## 九、总结

### 核心区别

| 比较项 | Java Stream flatMap | WebFlux flatMap |
|--------|---------------------|-----------------|
| **本质** | 扁平化嵌套集合 | 链式调用异步操作 |
| **执行** | 同步、阻塞 | 异步、非阻塞 |
| **场景** | 集合操作 | HTTP调用、数据库 |

### 关键要点

1. **Java Stream flatMap**：
   - 同步操作，立即执行
   - 用于扁平化嵌套集合
   - `Stream<List<T>>` → `Stream<T>`

2. **WebFlux flatMap**：
   - 异步操作，延迟执行
   - 用于链式调用异步操作
   - `Mono<T>` + (T → `Mono<R>`) → `Mono<R>`

3. **选择规则**：
   - 返回普通值 → 用 `map`
   - 返回 Mono/Flux → 用 `flatMap`

4. **常见错误**：
   - 应该用 flatMap 却用了 map（导致嵌套）
   - 在 flatMap 中使用阻塞操作（破坏异步模型）

---

## 十、实战练习

### 练习1：基础 flatMap

```java
// 任务：根据订单ID获取订单详情，然后获取用户信息
@GetMapping("/order/{id}/user")
public Mono<User> getOrderUser(@PathVariable Long id) {
    // TODO: 实现这个方法
    // 步骤1：调用 /api/orders/{id} 获取订单
    // 步骤2：使用 flatMap 调用 /api/users/{userId} 获取用户
}

// 答案：
@GetMapping("/order/{id}/user")
public Mono<User> getOrderUser(@PathVariable Long id) {
    return webClient.get()
            .uri("/api/orders/" + id)
            .retrieve()
            .bodyToMono(Order.class)
            .flatMap(order -> {
                return webClient.get()
                        .uri("/api/users/" + order.getUserId())
                        .retrieve()
                        .bodyToMono(User.class);
            });
}
```

### 练习2：map vs flatMap

```java
// 判断下面每个操作应该用 map 还是 flatMap

// 1. 将字符串转大写
Mono<String> result1 = Mono.just("hello")._____(s -> s.toUpperCase());
// 答案：map（返回 String）

// 2. 根据ID查询数据库
Mono<User> result2 = Mono.just(1)._____(id -> userRepository.findById(id));
// 答案：flatMap（返回 Mono<User>）

// 3. 计算字符串长度
Mono<Integer> result3 = Mono.just("hello")._____(s -> s.length());
// 答案：map（返回 int）

// 4. 调用外部服务
Mono<Response> result4 = Mono.just(1)._____(id -> webClient.get().uri("/api/" + id).retrieve().bodyToMono(Response.class));
// 答案：flatMap（返回 Mono<Response>）
```

---

现在你应该完全理解 `flatMap` 的作用和区别了！🎓

