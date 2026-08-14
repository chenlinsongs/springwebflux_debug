package org.example.springwebflux.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * flatMap 详解示例
 * 
 * 展示 Java Stream flatMap 和 WebFlux flatMap 的区别
 */
@RestController
@RequestMapping("/flatmap-demo")
public class FlatMapDemoController {

    @Autowired
    private WebClient webClient;

    // ============================================================
    // 一、Java Stream 的 flatMap（同步、扁平化集合）
    // ============================================================

    /**
     * 示例1：Java Stream flatMap - 扁平化嵌套集合
     * 
     * 访问：http://localhost:8080/flatmap-demo/stream-flatten
     */
    @GetMapping("/stream-flatten")
    public List<Integer> streamFlattenExample() {
        // 嵌套的列表
        List<List<Integer>> nested = Arrays.asList(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6),
            Arrays.asList(7, 8, 9)
        );
        
        // 不使用 flatMap：结果是 Stream<List<Integer>>
        Stream<List<Integer>> withoutFlatMap = nested.stream();
        // 无法直接得到所有数字
        
        // 使用 flatMap：扁平化成 Stream<Integer>
        List<Integer> flattened = nested.stream()
                .flatMap(list -> list.stream())  // List<Integer> → Stream<Integer>
                .collect(Collectors.toList());
        
        System.out.println("扁平化结果：" + flattened);
        // 输出：[1, 2, 3, 4, 5, 6, 7, 8, 9]
        
        return flattened;
    }

    /**
     * 示例2：Java Stream flatMap - 拆分字符串
     * 
     * 访问：http://localhost:8080/flatmap-demo/stream-split
     */
    @GetMapping("/stream-split")
    public List<String> streamSplitExample() {
        List<String> sentences = Arrays.asList(
            "Hello World",
            "Spring Boot",
            "WebFlux Demo"
        );
        
        // 使用 flatMap 将每个句子拆分成单词
        List<String> words = sentences.stream()
                .flatMap(sentence -> Arrays.stream(sentence.split(" ")))
                .collect(Collectors.toList());
        
        System.out.println("拆分结果：" + words);
        // 输出：[Hello, World, Spring, Boot, WebFlux, Demo]
        
        return words;
    }

    // ============================================================
    // 二、WebFlux 的 flatMap（异步、链式调用）
    // ============================================================

    /**
     * 示例3：map vs flatMap 的区别
     * 
     * 访问：http://localhost:8080/flatmap-demo/map-vs-flatmap
     */
    @GetMapping("/map-vs-flatmap")
    public Mono<String> mapVsFlatMapExample() {
        Mono<Integer> userId = Mono.just(1);
        
        // ✅ 使用 map：返回普通值
        Mono<String> withMap = userId.map(id -> {
            return "User-" + id;  // 返回 String
        });
        // 结果：Mono<String>
        
        // ❌ 如果用 map 返回 Mono，会嵌套
        // Mono<Mono<String>> wrong = userId.map(id -> {
        //     return Mono.just("User-" + id);  // 返回 Mono<String>
        // });
        // 结果：Mono<Mono<String>>，嵌套了！
        
        // ✅ 使用 flatMap：返回 Mono
        Mono<String> withFlatMap = userId.flatMap(id -> {
            return Mono.just("User-" + id);  // 返回 Mono<String>
        });
        // 结果：Mono<String>，扁平的
        
        return withFlatMap;
    }

    /**
     * 示例4：flatMap 的真正用途 - 链式调用异步操作
     * 
     * 访问：http://localhost:8080/flatmap-demo/chain-calls
     */
    @GetMapping("/chain-calls")
    public Mono<String> chainCallsExample() {
        System.out.println("开始链式调用，线程：" + Thread.currentThread().getName());
        
        return Mono.just(1)
                // 第1次 flatMap：模拟调用用户服务
                .flatMap(id -> {
                    System.out.println("调用用户服务，ID=" + id + "，线程：" + Thread.currentThread().getName());
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> "User(" + result + ")");
                })
                // 第2次 flatMap：模拟调用订单服务
                .flatMap(userData -> {
                    System.out.println("调用订单服务，用户=" + userData + "，线程：" + Thread.currentThread().getName());
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> userData + " + Orders(" + result + ")");
                })
                // 第3次 flatMap：模拟调用地址服务
                .flatMap(combined -> {
                    System.out.println("调用地址服务，数据=" + combined + "，线程：" + Thread.currentThread().getName());
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> combined + " + Address(" + result + ")");
                })
                .doOnSuccess(result -> {
                    System.out.println("完成，结果=" + result + "，线程：" + Thread.currentThread().getName());
                });
        
        // 关键：这个方法立即返回，不阻塞线程！
        // 3次HTTP调用会串行执行，但线程不会被阻塞
    }

    /**
     * 示例5：flatMap 实战 - 根据用户ID获取完整信息
     * 
     * 访问：http://localhost:8080/flatmap-demo/user-details/1
     */
    @GetMapping("/user-details/{id}")
    public Mono<String> getUserDetails(@PathVariable Long id) {
        return Mono.just(id)
                // 步骤1：获取用户基本信息
                .flatMap(userId -> {
                    System.out.println("步骤1：获取用户基本信息，ID=" + userId);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> "User(" + result + ")");
                })
                // 步骤2：根据用户信息获取订单列表
                .flatMap(userInfo -> {
                    System.out.println("步骤2：获取订单列表");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> userInfo + " -> Orders(" + result + ")");
                })
                // 步骤3：根据订单获取支付信息
                .flatMap(orderInfo -> {
                    System.out.println("步骤3：获取支付信息");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result -> orderInfo + " -> Payment(" + result + ")");
                });
        
        // 3个异步操作串行执行，但不阻塞线程
        // 每个操作完成后，自动触发下一个操作
    }

    /**
     * 示例6：错误示范 - 用 map 代替 flatMap
     * 
     * 访问：http://localhost:8080/flatmap-demo/wrong-map
     */
    @GetMapping("/wrong-map")
    public Mono<String> wrongMapExample() {
        Mono<Integer> userId = Mono.just(1);
        
        // ❌ 错误：应该用 flatMap 却用了 map
        Mono<Mono<String>> wrong = userId.map(id -> {
            // 返回 Mono<String>
            return webClient.get()
                    .uri("http://localhost:8080/comparison/mock-service")
                    .retrieve()
                    .bodyToMono(String.class);
        });
        // 结果：Mono<Mono<String>>，嵌套了！
        
        // ✅ 正确：使用 flatMap
        Mono<String> correct = userId.flatMap(id -> {
            return webClient.get()
                    .uri("http://localhost:8080/comparison/mock-service")
                    .retrieve()
                    .bodyToMono(String.class);
        });
        
        return correct.map(result -> "正确使用flatMap: " + result);
    }

    /**
     * 示例7：Flux 的 flatMap - 并发处理
     * 
     * 访问：http://localhost:8080/flatmap-demo/flux-flatmap
     */
    @GetMapping("/flux-flatmap")
    public Flux<String> fluxFlatMapExample() {
        // 有多个用户ID
        Flux<Integer> userIds = Flux.just(1, 2, 3, 4, 5);
        
        // 对每个用户ID，调用服务获取详情
        return userIds.flatMap(id -> {
            System.out.println("处理用户 " + id + "，线程：" + Thread.currentThread().getName());
            return webClient.get()
                    .uri("http://localhost:8080/comparison/mock-service")
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(result -> "User-" + id + ": " + result);
        });
        
        // 关键：这5个HTTP调用会并发执行！
        // 不是串行等待，而是同时发起
    }

    /**
     * 示例8：对比 Java Stream 和 WebFlux
     * 
     * 访问：http://localhost:8080/flatmap-demo/comparison
     */
    @GetMapping("/comparison")
    public Mono<String> comparisonExample() {
        return Mono.just(
            "=== Java Stream flatMap vs WebFlux flatMap ===\n\n" +
            
            "1. Java Stream flatMap:\n" +
            "   - 同步操作，立即执行\n" +
            "   - 用于扁平化嵌套集合\n" +
            "   - Stream<List<T>> → Stream<T>\n" +
            "   - 示例：list.stream().flatMap(sublist -> sublist.stream())\n\n" +
            
            "2. WebFlux flatMap:\n" +
            "   - 异步操作，延迟执行\n" +
            "   - 用于链式调用异步操作\n" +
            "   - Mono<T> + (T → Mono<R>) → Mono<R>\n" +
            "   - 示例：mono.flatMap(id -> webClient.get()...)\n\n" +
            
            "3. 选择规则:\n" +
            "   - 返回普通值 → 用 map\n" +
            "   - 返回 Mono/Flux → 用 flatMap\n\n" +
            
            "4. 记忆口诀:\n" +
            "   - 同步转换用 map\n" +
            "   - 异步调用 flatMap\n" +
            "   - 返回普通用 map\n" +
            "   - 返回 Mono 用 flatMap\n\n" +
            
            "测试接口:\n" +
            "- /flatmap-demo/stream-flatten - Java Stream 扁平化\n" +
            "- /flatmap-demo/map-vs-flatmap - map vs flatMap\n" +
            "- /flatmap-demo/chain-calls - 链式调用演示\n" +
            "- /flatmap-demo/user-details/1 - 实战示例\n" +
            "- /flatmap-demo/flux-flatmap - Flux 并发处理"
        );
    }
}

