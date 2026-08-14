package org.example.springwebflux.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * map vs flatMap 对比演示
 * 
 * 帮你理解两者的区别
 */
@RestController
@RequestMapping("/map-vs-flatmap")
public class MapVsFlatMapController {

    @Autowired
    private WebClient webClient;

    /**
     * 示例1：map 的基本用法
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/map-basic
     * 
     * map 用于同步转换，返回普通值
     */
    @GetMapping("/map-basic")
    public Mono<String> mapBasic() {
        return Mono.just(5)
                // map 1：数字 × 2
                .map(n -> {
                    System.out.println("map1: " + n + " → " + (n * 2));
                    return n * 2;  // 返回 int（普通值）
                })
                // map 2：转字符串
                .map(n -> {
                    String result = "结果是：" + n;
                    System.out.println("map2: " + n + " → " + result);
                    return result;  // 返回 String（普通值）
                })
                // map 3：添加前缀
                .map(s -> {
                    String result = "[SUCCESS] " + s;
                    System.out.println("map3: " + s + " → " + result);
                    return result;  // 返回 String（普通值）
                });
        
        // 所有 map 都返回普通值
        // 输出：[SUCCESS] 结果是：10
    }

    /**
     * 示例2：flatMap 的基本用法
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/flatmap-basic
     * 
     * flatMap 用于异步操作，返回 Mono/Flux
     */
    @GetMapping("/flatmap-basic")
    public Mono<String> flatMapBasic() {
        return Mono.just(1)
                // flatMap 1：调用服务1
                .flatMap(id -> {
                    System.out.println("flatMap1: 调用服务，id=" + id);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class);  // 返回 Mono<String>
                })
                // flatMap 2：调用服务2
                .flatMap(result1 -> {
                    System.out.println("flatMap2: 第一个结果=" + result1);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class);  // 返回 Mono<String>
                })
                // map：最后转换
                .map(result2 -> {
                    System.out.println("map: 最后处理，result2=" + result2);
                    return "最终结果：" + result2;  // 返回 String（普通值）
                });
        
        // flatMap 返回 Mono，map 返回普通值
    }

    /**
     * 示例3：错误示范 - 应该用 flatMap 却用了 map
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/wrong-map
     * 
     * 这会导致嵌套的 Mono
     */
    @GetMapping("/wrong-map")
    public Mono<String> wrongMap() {
        Mono<Integer> userId = Mono.just(1);
        
        // ❌ 错误：用 map 但返回 Mono
        // Mono<Mono<String>> nested = userId.map(id -> {
        //     return webClient.get()
        //             .uri("http://localhost:8080/comparison/mock-service")
        //             .retrieve()
        //             .bodyToMono(String.class);  // 返回 Mono<String>
        // });
        // 结果：Mono<Mono<String>>，嵌套了！
        
        // ✅ 正确：用 flatMap
        Mono<String> correct = userId.flatMap(id -> {
            return webClient.get()
                    .uri("http://localhost:8080/comparison/mock-service")
                    .retrieve()
                    .bodyToMono(String.class);  // 返回 Mono<String>
        });
        // 结果：Mono<String>，扁平的
        
        return correct.map(result -> "正确使用flatMap: " + result);
    }

    /**
     * 示例4：对比 map 和 flatMap
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/compare
     */
    @GetMapping("/compare")
    public Mono<String> compare() {
        System.out.println("\n=== 开始对比 ===");
        
        return Mono.just(5)
                // ✅ map：返回普通值
                .map(n -> {
                    int result = n * 2;
                    System.out.println("1. map返回普通值: " + n + " → " + result);
                    return result;  // 返回 int
                })
                
                // ✅ flatMap：返回 Mono
                .flatMap(n -> {
                    System.out.println("2. flatMap返回Mono: " + n);
                    return Mono.just("Number-" + n);  // 返回 Mono<String>
                })
                
                // ✅ map：返回普通值
                .map(s -> {
                    String result = s.toUpperCase();
                    System.out.println("3. map返回普通值: " + s + " → " + result);
                    return result;  // 返回 String
                })
                
                // ✅ flatMap：调用外部服务
                .flatMap(s -> {
                    System.out.println("4. flatMap调用服务: " + s);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class);  // 返回 Mono<String>
                })
                
                // ✅ map：最终处理
                .map(result -> {
                    String finalResult = "最终结果：" + result;
                    System.out.println("5. map最终处理: " + finalResult);
                    return finalResult;  // 返回 String
                });
    }

    /**
     * 示例5：实战场景 - 获取用户订单
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/user-orders/123
     * 
     * 混合使用 map 和 flatMap
     */
    @GetMapping("/user-orders/{userId}")
    public Mono<String> getUserOrders(@PathVariable String userId) {
        System.out.println("\n=== 获取用户订单 ===");
        
        return Mono.just(userId)
                // map：转换ID（同步操作）
                .map(id -> {
                    System.out.println("1. map - 转换ID: " + id);
                    return "USER-" + id;  // 返回 String
                })
                
                // flatMap：调用用户服务（异步操作）
                .flatMap(id -> {
                    System.out.println("2. flatMap - 调用用户服务: " + id);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)  // 返回 Mono<String>
                            .map(result -> "用户信息[" + result + "]");
                })
                
                // map：提取用户信息（同步操作）
                .map(userInfo -> {
                    System.out.println("3. map - 提取用户信息: " + userInfo);
                    return userInfo + " -> 查询订单";  // 返回 String
                })
                
                // flatMap：调用订单服务（异步操作）
                .flatMap(info -> {
                    System.out.println("4. flatMap - 调用订单服务: " + info);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)  // 返回 Mono<String>
                            .map(result -> info + " -> 订单[" + result + "]");
                })
                
                // map：格式化结果（同步操作）
                .map(finalResult -> {
                    System.out.println("5. map - 格式化结果: " + finalResult);
                    return "【完成】" + finalResult;  // 返回 String
                });
    }

    /**
     * 示例6：链式调用演示
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/chain
     * 
     * 3个服务串行调用
     */
    @GetMapping("/chain")
    public Mono<String> chain() {
        System.out.println("\n=== 链式调用3个服务 ===");
        
        long start = System.currentTimeMillis();
        
        return Mono.just("开始")
                // flatMap 1：调用服务1
                .flatMap(msg -> {
                    System.out.println("1. 调用服务1: " + msg);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> msg + " → 服务1[" + result + "]");
                })
                
                // flatMap 2：调用服务2
                .flatMap(msg -> {
                    System.out.println("2. 调用服务2: " + msg);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> msg + " → 服务2[" + result + "]");
                })
                
                // flatMap 3：调用服务3
                .flatMap(msg -> {
                    System.out.println("3. 调用服务3: " + msg);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> msg + " → 服务3[" + result + "]");
                })
                
                // map：添加耗时
                .map(result -> {
                    long end = System.currentTimeMillis();
                    return result + "\n\n总耗时：" + (end - start) + "ms（串行但不阻塞线程）";
                });
    }

    /**
     * 示例7：类型对比
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/types
     */
    @GetMapping("/types")
    public Mono<String> types() {
        return Mono.just(
                "=== map vs flatMap 类型对比 ===\n\n" +
                
                "1. map 的类型：\n" +
                "   Mono<T>.map(T → R) → Mono<R>\n" +
                "   例：Mono<Int>.map(n → String) → Mono<String>\n\n" +
                
                "2. flatMap 的类型：\n" +
                "   Mono<T>.flatMap(T → Mono<R>) → Mono<R>\n" +
                "   例：Mono<Int>.flatMap(n → Mono<String>) → Mono<String>\n\n" +
                
                "3. 区别：\n" +
                "   map:     转换函数返回 R（普通值）\n" +
                "   flatMap: 转换函数返回 Mono<R>\n\n" +
                
                "4. 如果用错：\n" +
                "   map 返回 Mono → Mono<Mono<R>> 嵌套！\n" +
                "   flatMap 返回普通值 → 没必要，用 map 更好\n\n" +
                
                "5. 记忆口诀：\n" +
                "   返回普通值 → 用 map\n" +
                "   返回 Mono/Flux → 用 flatMap\n\n" +
                
                "6. 典型场景：\n" +
                "   map:     计算、转换、格式化\n" +
                "   flatMap: HTTP调用、数据库查询、外部服务\n\n" +
                
                "测试接口：\n" +
                "- /map-vs-flatmap/map-basic - map示例\n" +
                "- /map-vs-flatmap/flatmap-basic - flatMap示例\n" +
                "- /map-vs-flatmap/compare - 对比演示\n" +
                "- /map-vs-flatmap/chain - 链式调用\n" +
                "- /map-vs-flatmap/user-orders/123 - 实战场景"
        );
    }

    /**
     * 示例8：决策树
     * 
     * 访问：http://localhost:8080/map-vs-flatmap/decision
     */
    @GetMapping("/decision")
    public Mono<String> decision() {
        return Mono.just(
                "=== 如何选择 map 还是 flatMap ===\n\n" +
                
                "问题：转换函数返回什么？\n\n" +
                
                "情况1：返回普通值\n" +
                "├─ int, long, double\n" +
                "├─ String, Boolean\n" +
                "├─ User, Order（自定义对象）\n" +
                "└─ List<T>, Map<K,V>\n" +
                "   → 使用 map\n\n" +
                
                "情况2：返回 Mono/Flux\n" +
                "├─ Mono.just(...)\n" +
                "├─ webClient.get()...\n" +
                "├─ repository.findById(...)\n" +
                "├─ redisTemplate.get(...)\n" +
                "└─ 任何返回 Mono/Flux 的方法\n" +
                "   → 使用 flatMap\n\n" +
                
                "快速判断：\n" +
                "```java\n" +
                "mono.???(x -> ...)\n" +
                "           ↑\n" +
                "      看这里返回什么\n" +
                "\n" +
                "// 如果是：return 5;\n" +
                "// 如果是：return \"hello\";\n" +
                "// 如果是：return new User();\n" +
                "→ 用 map\n" +
                "\n" +
                "// 如果是：return Mono.just(...);\n" +
                "// 如果是：return webClient.get()...;\n" +
                "// 如果是：return repository.find(...);\n" +
                "→ 用 flatMap\n" +
                "```\n\n" +
                
                "常见错误：\n" +
                "❌ map 返回 Mono → 导致嵌套 Mono<Mono<T>>\n" +
                "❌ flatMap 返回普通值 → 需要不必要地包装\n" +
                "✅ map 返回普通值 → 正确\n" +
                "✅ flatMap 返回 Mono → 正确"
        );
    }
}

