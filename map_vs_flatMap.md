# map vs flatMap - 一看就懂

## 🎯 核心区别（一句话）

**map**：转换函数返回**普通值**  
**flatMap**：转换函数返回 **Mono/Flux**（异步操作）

---

## 📊 对比表格

| 特性 | map | flatMap |
|------|-----|---------|
| **转换函数返回** | 普通值（T → R） | Mono/Flux（T → Mono\<R>） |
| **结果类型** | Mono\<R> | Mono\<R> |
| **用途** | 同步转换 | 异步操作、链式调用 |
| **典型场景** | 数据转换、计算 | HTTP调用、数据库查询 |
| **是否"扁平化"** | 否 | 是 |

---

## 💡 简单示例

### 示例1：map - 返回普通值

```java
// 场景：将数字转换为字符串
Mono<Integer> number = Mono.just(5);

// ✅ 使用 map：返回普通值 String
Mono<String> result = number.map(n -> "数字是：" + n);
//                           ↑
//                      返回 String（普通值）

// 结果：Mono<String>
```

### 示例2：flatMap - 返回 Mono

```java
// 场景：根据ID查询数据库
Mono<Integer> userId = Mono.just(1);

// ✅ 使用 flatMap：返回 Mono<User>
Mono<User> result = userId.flatMap(id -> {
    return userRepository.findById(id);  // 返回 Mono<User>
//         ↑
//    返回 Mono（异步操作）
});

// 结果：Mono<User>
```

---

## ❌ 如果用错了会怎样？

### 错误1：应该用 flatMap 却用了 map

```java
Mono<Integer> userId = Mono.just(1);

// ❌ 错误：用 map 但返回 Mono
Mono<Mono<User>> wrong = userId.map(id -> {
    return userRepository.findById(id);  // 返回 Mono<User>
});
// 结果：Mono<Mono<User>> ← 嵌套了！无法使用

// ✅ 正确：用 flatMap
Mono<User> correct = userId.flatMap(id -> {
    return userRepository.findById(id);  // 返回 Mono<User>
});
// 结果：Mono<User> ← 扁平的，可以使用
```

### 错误2：应该用 map 却用了 flatMap

```java
Mono<String> name = Mono.just("hello");

// ❌ 不必要：用 flatMap 但返回普通值
Mono<String> wrong = name.flatMap(s -> {
    return Mono.just(s.toUpperCase());  // 没必要包装成 Mono
});

// ✅ 正确：用 map
Mono<String> correct = name.map(s -> s.toUpperCase());
// 更简洁，更高效
```

---

## 🔍 详细对比

### 场景1：数据转换（用 map）

```java
// 任务：将字符串转为大写
Mono<String> input = Mono.just("hello");

// ✅ 使用 map
Mono<String> output = input.map(s -> s.toUpperCase());
// "hello" → "HELLO"

// 其他例子：
Mono.just("hello").map(s -> s.length());           // String → int
Mono.just(5).map(n -> n * 2);                      // int → int
Mono.just("123").map(s -> Integer.parseInt(s));    // String → int
```

### 场景2：调用服务（用 flatMap）

```java
// 任务：根据ID调用用户服务
Mono<Integer> userId = Mono.just(1);

// ✅ 使用 flatMap
Mono<User> user = userId.flatMap(id -> {
    return webClient.get()
            .uri("/api/users/" + id)
            .retrieve()
            .bodyToMono(User.class);  // 返回 Mono<User>
});

// 其他例子：
userId.flatMap(id -> userRepository.findById(id));        // 数据库查询
userId.flatMap(id -> redisTemplate.get("user:" + id));    // Redis查询
userId.flatMap(id -> callExternalService(id));            // 外部服务调用
```

---

## 🎨 可视化理解

### map 的工作流程

```
输入：Mono<Integer> = 5

  Mono.just(5)
      ↓
  .map(n -> n * 2)
      ↓
    [执行函数]
      ↓
    返回 10（普通值）
      ↓
  包装成 Mono<Integer>
      ↓
输出：Mono<Integer> = 10

关键：函数返回普通值，map 自动包装成 Mono
```

### flatMap 的工作流程

```
输入：Mono<Integer> = 1

  Mono.just(1)
      ↓
  .flatMap(id -> webClient.get()...)
      ↓
    [执行函数]
      ↓
    返回 Mono<User>（已经是 Mono）
      ↓
  flatMap 直接返回这个 Mono（不再包装）
      ↓
输出：Mono<User>

关键：函数返回 Mono，flatMap 直接使用，不重复包装
```

---

## 🧮 类型推导

### map 的类型

```java
// map 的签名
<R> Mono<R> map(Function<T, R> mapper)

// 例子
Mono<String> mono = Mono.just(5)
    .map(n -> "Number: " + n);
//       ↑          ↑
//      T=int    R=String

// T → R
// Mono<T> → Mono<R>
```

### flatMap 的类型

```java
// flatMap 的签名
<R> Mono<R> flatMap(Function<T, Mono<R>> mapper)

// 例子
Mono<User> mono = Mono.just(1)
    .flatMap(id -> userRepository.findById(id));
//           ↑                    ↑
//         T=int              Mono<User>

// T → Mono<R>
// Mono<T> → Mono<R>（扁平化）
```

---

## 📝 实战示例

### 示例1：获取用户信息并转换

```java
@GetMapping("/user/{id}")
public Mono<UserDTO> getUser(@PathVariable Long id) {
    return Mono.just(id)
            // flatMap：调用数据库（返回 Mono）
            .flatMap(userId -> userRepository.findById(userId))
            // map：转换为 DTO（返回普通值）
            .map(user -> {
                UserDTO dto = new UserDTO();
                dto.setName(user.getName());
                dto.setEmail(user.getEmail());
                return dto;  // 返回普通对象
            });
}
```

### 示例2：连续调用多个服务

```java
@GetMapping("/order/{id}/details")
public Mono<OrderDetails> getOrderDetails(@PathVariable Long id) {
    return Mono.just(id)
            // flatMap：查询订单
            .flatMap(orderId -> orderRepository.findById(orderId))
            // map：提取用户ID
            .map(order -> order.getUserId())
            // flatMap：查询用户
            .flatMap(userId -> userRepository.findById(userId))
            // map：组合结果
            .map(user -> new OrderDetails(order, user));
}
```

### 示例3：链式调用

```java
public Mono<String> processOrder(Long orderId) {
    return Mono.just(orderId)
            // flatMap：获取订单
            .flatMap(id -> getOrder(id))              // 返回 Mono<Order>
            
            // map：校验订单
            .map(order -> validateOrder(order))       // 返回 Order
            
            // flatMap：扣减库存
            .flatMap(order -> reduceStock(order))     // 返回 Mono<Order>
            
            // flatMap：创建支付
            .flatMap(order -> createPayment(order))   // 返回 Mono<Payment>
            
            // map：生成结果
            .map(payment -> "Success: " + payment);   // 返回 String
}
```

---

## 🤔 如何选择？

### 决策树

```
转换函数返回什么？
├─ 返回普通值（int, String, User 等）
│  └─ 使用 map
│
└─ 返回 Mono/Flux（异步操作）
   └─ 使用 flatMap
```

### 快速判断

```java
// 问：这个操作返回什么？
someValue.???(x -> ...)
           ↑
      看这里返回什么

// 如果返回：5, "hello", new User() → 用 map
// 如果返回：Mono.just(5), webClient.get()..., repository.find() → 用 flatMap
```

---

## 💻 完整示例代码

### map 示例

```java
@GetMapping("/demo/map")
public Mono<String> mapDemo() {
    return Mono.just(100)
            // map 1：计算
            .map(n -> n * 2)                    // 100 → 200
            // map 2：转字符串
            .map(n -> "结果是：" + n)           // 200 → "结果是：200"
            // map 3：添加前缀
            .map(s -> "[SUCCESS] " + s);       // → "[SUCCESS] 结果是：200"
    
    // 所有 map 都返回普通值
}
```

### flatMap 示例

```java
@GetMapping("/demo/flatmap/{userId}")
public Mono<String> flatMapDemo(@PathVariable Long userId) {
    return Mono.just(userId)
            // flatMap 1：查询用户
            .flatMap(id -> {
                return webClient.get()
                        .uri("/api/users/" + id)
                        .retrieve()
                        .bodyToMono(User.class);  // 返回 Mono<User>
            })
            // flatMap 2：查询订单
            .flatMap(user -> {
                return webClient.get()
                        .uri("/api/orders?userId=" + user.getId())
                        .retrieve()
                        .bodyToMono(Order.class);  // 返回 Mono<Order>
            })
            // map：组合结果
            .map(order -> "订单号：" + order.getId());  // 返回 String
    
    // flatMap 返回 Mono，map 返回普通值
}
```

### 混合使用示例

```java
@GetMapping("/demo/mixed/{id}")
public Mono<OrderSummary> mixedDemo(@PathVariable Long id) {
    return Mono.just(id)
            // flatMap：异步查询
            .flatMap(orderId -> orderService.findById(orderId))
            
            // map：同步转换
            .map(order -> {
                order.setStatus("PROCESSING");
                return order;
            })
            
            // flatMap：异步保存
            .flatMap(order -> orderService.save(order))
            
            // map：转换为 DTO
            .map(order -> new OrderSummary(order))
            
            // map：添加时间戳
            .map(summary -> {
                summary.setTimestamp(System.currentTimeMillis());
                return summary;
            });
}
```

---

## 🎯 记忆口诀

```
同步转换用 map，
异步调用 flatMap。

返回普通用 map，
返回 Mono 用 flatMap。

map 包装结果，
flatMap 扁平化。
```

---

## 📋 常见场景

| 场景 | 使用 | 示例 |
|------|------|------|
| 字符串转大写 | map | `.map(s -> s.toUpperCase())` |
| 计算数值 | map | `.map(n -> n * 2)` |
| 对象转 DTO | map | `.map(user -> new UserDTO(user))` |
| 调用 WebClient | flatMap | `.flatMap(id -> webClient.get()...)` |
| 数据库查询 | flatMap | `.flatMap(id -> repository.find(id))` |
| Redis 查询 | flatMap | `.flatMap(key -> redis.get(key))` |
| 多个服务串行调用 | flatMap | `.flatMap(...).flatMap(...)` |

---

## 🚨 常见错误

### 错误1：嵌套 Mono

```java
// ❌ 错误
Mono<Mono<User>> wrong = userId.map(id -> userRepository.findById(id));

// ✅ 正确
Mono<User> correct = userId.flatMap(id -> userRepository.findById(id));
```

### 错误2：不必要的包装

```java
// ❌ 错误
Mono<String> wrong = name.flatMap(s -> Mono.just(s.toUpperCase()));

// ✅ 正确
Mono<String> correct = name.map(s -> s.toUpperCase());
```

### 错误3：在 map 中阻塞

```java
// ❌ 错误：在 map 中阻塞
Mono<User> wrong = userId.map(id -> {
    // 使用 RestTemplate（阻塞）
    return restTemplate.getForObject("/users/" + id, User.class);
});

// ✅ 正确：用 flatMap + WebClient（非阻塞）
Mono<User> correct = userId.flatMap(id -> {
    return webClient.get().uri("/users/" + id).retrieve().bodyToMono(User.class);
});
```

---

## 📚 总结

### 核心区别

```
map:
  - 转换函数：T → R（普通值）
  - 自动包装结果
  - 用于同步转换

flatMap:
  - 转换函数：T → Mono<R>
  - 扁平化结果
  - 用于异步操作
```

### 选择规则

```
1. 看转换函数返回什么
   - 返回普通值 → map
   - 返回 Mono/Flux → flatMap

2. 看操作类型
   - 同步操作（计算、转换） → map
   - 异步操作（HTTP、数据库） → flatMap

3. 避免嵌套
   - 如果出现 Mono<Mono<T>> → 用 flatMap
```

---

## 🧪 快速测试

启动项目后访问这些接口：

```bash
# map 示例
curl http://localhost:8080/flatmap-demo/map-vs-flatmap

# flatMap 链式调用
curl http://localhost:8080/flatmap-demo/chain-calls

# 混合使用
curl http://localhost:8080/flatmap-demo/user-details/1
```

---

现在你应该完全理解 map 和 flatMap 的区别了！

**关键记住**：
- **map**：返回普通值，用于同步转换
- **flatMap**：返回 Mono/Flux，用于异步操作

🎯

