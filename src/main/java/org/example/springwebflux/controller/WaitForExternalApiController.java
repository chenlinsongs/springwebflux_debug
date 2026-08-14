package org.example.springwebflux.controller;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * 演示如何返回依赖外部接口的 Mono
 * 
 * 展示等待外部接口、组合调用、并行调用等场景
 */
@RestController
@RequestMapping("/wait-external")
public class WaitForExternalApiController {

    @Autowired
    private WebClient webClient;

    /**
     * 场景1：直接返回外部接口的 Mono（最简单）
     * 
     * 访问：http://localhost:8080/wait-external/simple
     * 
     * 这是最常见的场景：你的接口依赖另一个服务
     */
    @GetMapping("/simple")
    public Mono<String> simple() {
        System.out.println("\n=== 场景1：直接返回外部接口的 Mono ===");
        System.out.println("1. Controller 方法被调用");
        
        // 直接返回 WebClient 的 Mono
        // 框架会自动等待外部接口响应
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")  // 调用模拟服务
                .retrieve()
                .bodyToMono(String.class)
                .doOnSubscribe(sub -> 
                    System.out.println("2. 框架 subscribe，发起 HTTP 请求"))
                .doOnNext(result -> 
                    System.out.println("3. 收到外部接口响应：" + result))
                .map(result -> "来自外部接口的数据：" + result);
        
        // 流程：
        // Controller 返回 Mono → 框架 subscribe → 发起请求 → 等待响应 → 写回客户端
    }

    /**
     * 场景2：调用外部接口并处理结果
     * 
     * 访问：http://localhost:8080/wait-external/with-processing
     */
    @GetMapping("/with-processing")
    public Mono<ProcessedData> withProcessing() {
        System.out.println("\n=== 场景2：调用外部接口并处理结果 ===");
        
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(raw -> System.out.println("收到原始数据：" + raw))
                // 处理外部接口的数据
                .map(raw -> {
                    System.out.println("处理数据...");
                    ProcessedData data = new ProcessedData();
                    data.setOriginal(raw);
                    data.setProcessed(raw.toUpperCase());
                    data.setTimestamp(System.currentTimeMillis());
                    return data;
                })
                .doOnNext(processed -> 
                    System.out.println("处理完成：" + processed));
    }

    /**
     * 场景3：串行调用多个外部接口
     * 
     * 访问：http://localhost:8080/wait-external/sequential
     */
    @GetMapping("/sequential")
    public Mono<String> sequential() {
        System.out.println("\n=== 场景3：串行调用多个外部接口 ===");
        
        long start = System.currentTimeMillis();
        
        return webClient.get()
                // 调用第1个接口
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(result1 -> 
                    System.out.println("第1个接口响应：" + result1))
                
                // 用第1个接口的结果调用第2个接口
                .flatMap(result1 -> {
                    System.out.println("用第1个结果调用第2个接口");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result2 -> result1 + " → " + result2);
                })
                .doOnNext(combined -> 
                    System.out.println("第2个接口响应，组合结果：" + combined))
                
                // 用前面的结果调用第3个接口
                .flatMap(combined -> {
                    System.out.println("用组合结果调用第3个接口");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result3 -> combined + " → " + result3);
                })
                
                .map(finalResult -> {
                    long end = System.currentTimeMillis();
                    return "串行调用完成：" + finalResult + 
                           "\n总耗时：" + (end - start) + "ms（约600ms）";
                });
        
        // 3个接口串行调用：200ms + 200ms + 200ms = 600ms
    }

    /**
     * 场景4：并行调用多个外部接口
     * 
     * 访问：http://localhost:8080/wait-external/parallel
     */
    @GetMapping("/parallel")
    public Mono<String> parallel() {
        System.out.println("\n=== 场景4：并行调用多个外部接口 ===");
        
        long start = System.currentTimeMillis();
        
        // 同时发起3个请求
        System.out.println("同时发起3个请求");
        
        Mono<String> call1 = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(r -> System.out.println("接口1响应：" + r));
        
        Mono<String> call2 = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(r -> System.out.println("接口2响应：" + r));
        
        Mono<String> call3 = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(r -> System.out.println("接口3响应：" + r));
        
        // 并行执行，等待所有完成
        return Mono.zip(call1, call2, call3)
                .map(tuple -> {
                    long end = System.currentTimeMillis();
                    String result1 = tuple.getT1();
                    String result2 = tuple.getT2();
                    String result3 = tuple.getT3();
                    return "并行调用完成：[" + result1 + ", " + result2 + ", " + result3 + "]\n" +
                           "总耗时：" + (end - start) + "ms（约200ms）\n" +
                           "节省时间：400ms（66%）";
                });
        
        // 3个接口并行调用：max(200ms, 200ms, 200ms) = 200ms
    }

    /**
     * 场景5：带超时和错误处理
     * 
     * 访问：http://localhost:8080/wait-external/with-timeout
     */
    @GetMapping("/with-timeout")
    public Mono<String> withTimeout() {
        System.out.println("\n=== 场景5：带超时和错误处理 ===");
        
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                // 设置超时
                .timeout(Duration.ofSeconds(5))
                .doOnNext(result -> System.out.println("成功：" + result))
                // 错误处理
                .onErrorResume(error -> {
                    System.err.println("发生错误：" + error.getClass().getSimpleName());
                    return Mono.just("默认值（服务不可用）");
                });
    }

    /**
     * 场景6：有条件的调用
     * 
     * 访问：http://localhost:8080/wait-external/conditional/{type}
     * 
     * 示例：
     * - /wait-external/conditional/vip
     * - /wait-external/conditional/normal
     */
    @GetMapping("/conditional/{type}")
    public Mono<String> conditional(@PathVariable String type) {
        System.out.println("\n=== 场景6：有条件的调用，类型=" + type + " ===");
        
        // 先调用第1个接口
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(result -> System.out.println("第1个接口：" + result))
                
                // 根据条件决定是否调用第2个接口
                .flatMap(result -> {
                    if ("vip".equals(type)) {
                        System.out.println("VIP用户，调用额外接口");
                        return webClient.get()
                                .uri("http://localhost:8080/comparison/mock-service")
                                .retrieve()
                                .bodyToMono(String.class)
                                .map(extra -> result + " + VIP额外数据(" + extra + ")");
                    } else {
                        System.out.println("普通用户，不调用额外接口");
                        return Mono.just(result);
                    }
                });
    }

    /**
     * 场景7：重试机制
     * 
     * 访问：http://localhost:8080/wait-external/with-retry
     */
    @GetMapping("/with-retry")
    public Mono<String> withRetry() {
        System.out.println("\n=== 场景7：重试机制 ===");
        
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                // 失败时重试3次，每次间隔1秒
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .doBeforeRetry(signal -> 
                            System.out.println("重试第" + signal.totalRetries() + "次")))
                .doOnNext(result -> System.out.println("成功：" + result))
                // 重试后仍失败，返回默认值
                .onErrorResume(error -> {
                    System.err.println("重试3次后仍失败");
                    return Mono.just("默认值");
                });
    }

    /**
     * 场景8：使用 Mono.defer
     * 
     * 访问：http://localhost:8080/wait-external/with-defer
     */
    @GetMapping("/with-defer")
    public Mono<String> withDefer() {
        System.out.println("\n=== 场景8：使用 Mono.defer ===");
        System.out.println("1. Controller 方法被调用");
        
        // Mono.defer 延迟创建 Mono
        return Mono.defer(() -> {
            System.out.println("2. Mono.defer 被执行（subscribe时）");
            return webClient.get()
                    .uri("http://localhost:8080/comparison/mock-service")
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(result -> "Defer结果：" + result);
        });
    }

    /**
     * 场景9：模拟复杂业务流程
     * 
     * 访问：http://localhost:8080/wait-external/complex-flow
     */
    @GetMapping("/complex-flow")
    public Mono<ComplexResult> complexFlow() {
        System.out.println("\n=== 场景9：复杂业务流程 ===");
        
        return webClient.get()
                // 步骤1：获取用户信息
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(user -> System.out.println("步骤1：获取用户 = " + user))
                
                // 步骤2：根据用户信息，并行获取订单和地址
                .flatMap(user -> {
                    System.out.println("步骤2：并行获取订单和地址");
                    
                    Mono<String> ordersMono = webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnNext(o -> System.out.println("  订单数据：" + o));
                    
                    Mono<String> addressMono = webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnNext(a -> System.out.println("  地址数据：" + a));
                    
                    return Mono.zip(ordersMono, addressMono)
                            .map(tuple -> new ComplexResult(user, tuple.getT1(), tuple.getT2()));
                })
                
                // 步骤3：根据订单，获取支付信息
                .flatMap(result -> {
                    System.out.println("步骤3：获取支付信息");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(payment -> {
                                result.setPayment(payment);
                                return result;
                            });
                })
                
                .doOnNext(finalResult -> 
                    System.out.println("完成：" + finalResult));
    }

    /**
     * 场景10：错误示范 - 使用 block()
     * 
     * 访问：http://localhost:8080/wait-external/wrong-with-block
     */
    @GetMapping("/wrong-with-block")
    public Mono<String> wrongWithBlock() {
        System.out.println("\n=== ❌ 错误示范：使用 block() ===");
        
        Mono<String> mono = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class);
        
        // ❌ 错误：使用 block() 阻塞线程
        System.out.println("调用 block()，线程阻塞...");
        String result = mono.block();  // 阻塞当前线程
        System.out.println("block() 返回，线程释放");
        
        return Mono.just("❌ 错误做法：使用了 block()，线程被阻塞\n结果：" + result);
        
        // 问题：破坏了异步非阻塞模型
    }

    /**
     * 场景11：正确做法 - 直接返回 Mono
     * 
     * 访问：http://localhost:8080/wait-external/correct-no-block
     */
    @GetMapping("/correct-no-block")
    public Mono<String> correctNoBlock() {
        System.out.println("\n=== ✅ 正确做法：直接返回 Mono ===");
        
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .map(result -> "✅ 正确做法：直接返回 Mono\n结果：" + result);
        
        // 优点：异步非阻塞，线程不等待
    }

    /**
     * 总结接口
     */
    @GetMapping("/summary")
    public Mono<String> summary() {
        return Mono.just(
                "=== 等待外部接口返回 Mono 总结 ===\n\n" +
                
                "核心要点：\n" +
                "1. 直接返回 WebClient 的 Mono\n" +
                "2. 框架会自动等待外部接口响应\n" +
                "3. 不需要手动等待，不要使用 block()\n\n" +
                
                "场景示例：\n" +
                "- /wait-external/simple - 最简单的场景\n" +
                "- /wait-external/sequential - 串行调用\n" +
                "- /wait-external/parallel - 并行调用 ⭐\n" +
                "- /wait-external/with-timeout - 超时处理\n" +
                "- /wait-external/conditional/vip - 条件调用\n" +
                "- /wait-external/with-retry - 重试机制\n" +
                "- /wait-external/complex-flow - 复杂流程\n\n" +
                
                "错误 vs 正确：\n" +
                "- /wait-external/wrong-with-block - ❌ 使用 block()\n" +
                "- /wait-external/correct-no-block - ✅ 直接返回 Mono\n\n" +
                
                "记住：\n" +
                "✅ return webClient.get()...\n" +
                "✅ 串行用 flatMap\n" +
                "✅ 并行用 Mono.zip\n" +
                "❌ 不要用 block()"
        );
    }

    // 辅助类
    static class ProcessedData {
        private String original;
        private String processed;
        private long timestamp;

        public String getOriginal() { return original; }
        public void setOriginal(String original) { this.original = original; }

        public String getProcessed() { return processed; }
        public void setProcessed(String processed) { this.processed = processed; }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        @Override
        public String toString() {
            return "ProcessedData{original='" + original + "', processed='" + processed + "', timestamp=" + timestamp + "}";
        }
    }

    static class ComplexResult {
        private String user;
        private String orders;
        private String address;
        private String payment;

        public ComplexResult(String user, String orders, String address) {
            this.user = user;
            this.orders = orders;
            this.address = address;
        }

        public void setPayment(String payment) {
            this.payment = payment;
        }

        @Override
        public String toString() {
            return "ComplexResult{user='" + user + "', orders='" + orders + "', address='" + address + "', payment='" + payment + "'}";
        }
    }
}

