package org.example.springwebflux.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 性能对比演示：Servlet vs WebFlux
 * 
 * 访问这些接口可以直观感受两种模式的区别
 */
@RestController
@RequestMapping("/comparison")
public class PerformanceComparisonController {

    @Autowired
    private WebClient webClient;

    private final AtomicInteger requestCounter = new AtomicInteger(0);

    /**
     * 模拟外部服务：延迟200ms返回数据
     * 这个接口用来模拟慢速的外部服务
     */
    @GetMapping("/mock-service")
    public Mono<String> mockService() {
        int requestId = requestCounter.incrementAndGet();
        String threadName = Thread.currentThread().getName();
        System.out.println("[请求 " + requestId + "] 模拟服务开始处理，线程：" + threadName);
        
        return Mono.delay(Duration.ofMillis(200))  // 模拟IO延迟
                .map(tick -> {
                    String completionThread = Thread.currentThread().getName();
                    System.out.println("[请求 " + requestId + "] 模拟服务完成，线程：" + completionThread);
                    return "数据-" + requestId;
                });
    }

    /**
     * 演示1：单个请求 - WebFlux方式
     * 
     * 访问：http://localhost:8080/comparison/webflux-single
     * 
     * 观察点：
     * 1. 方法立即返回
     * 2. 线程名称在不同阶段可能不同
     */
    @GetMapping("/webflux-single")
    public Mono<String> webfluxSingle() {
        long start = System.currentTimeMillis();
        String startThread = Thread.currentThread().getName();
        System.out.println("【WebFlux】开始处理请求，线程：" + startThread);
        
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .map(result -> {
                    long end = System.currentTimeMillis();
                    String endThread = Thread.currentThread().getName();
                    String response = String.format(
                            "【WebFlux】结果：%s | 耗时：%dms | 开始线程：%s | 结束线程：%s",
                            result, (end - start), startThread, endThread
                    );
                    System.out.println(response);
                    return response;
                });
        
        // 这个方法会立即返回，不会等待200ms！
    }

    /**
     * 演示2：串行调用3个服务 - WebFlux方式
     * 
     * 访问：http://localhost:8080/comparison/webflux-sequential
     * 
     * 耗时：约 600ms（200 + 200 + 200）
     */
    @GetMapping("/webflux-sequential")
    public Mono<String> webfluxSequential() {
        long start = System.currentTimeMillis();
        
        return webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class)
                .flatMap(result1 -> {
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result2 -> result1 + ", " + result2);
                })
                .flatMap(combined -> {
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result3 -> combined + ", " + result3);
                })
                .map(finalResult -> {
                    long end = System.currentTimeMillis();
                    return String.format("【串行】结果：%s | 总耗时：%dms", finalResult, (end - start));
                });
    }

    /**
     * 演示3：并行调用3个服务 - WebFlux方式
     * 
     * 访问：http://localhost:8080/comparison/webflux-parallel
     * 
     * 耗时：约 200ms（并行执行，取最大值）
     * 
     * 这是WebFlux的核心优势！
     */
    @GetMapping("/webflux-parallel")
    public Mono<String> webfluxParallel() {
        long start = System.currentTimeMillis();
        
        // 创建3个并行的异步调用
        Mono<String> call1 = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class);
        
        Mono<String> call2 = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class);
        
        Mono<String> call3 = webClient.get()
                .uri("http://localhost:8080/comparison/mock-service")
                .retrieve()
                .bodyToMono(String.class);
        
        // 并行执行，等待所有完成
        return Mono.zip(call1, call2, call3)
                .map(tuple -> {
                    long end = System.currentTimeMillis();
                    String result = String.format("%s, %s, %s", 
                            tuple.getT1(), tuple.getT2(), tuple.getT3());
                    return String.format("【并行】结果：%s | 总耗时：%dms（节省了约400ms！）", 
                            result, (end - start));
                });
    }

    /**
     * 演示4：模拟阻塞操作（错误示范）
     * 
     * 访问：http://localhost:8080/comparison/wrong-blocking
     * 
     * 警告：这样做会破坏WebFlux的异步模型！
     */
    @GetMapping("/wrong-blocking")
    public Mono<String> wrongBlocking() {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("【错误】在EventLoop线程中阻塞！线程：" + threadName);
            
            // ❌ 错误：阻塞EventLoop线程
            Thread.sleep(1000);
            
            return "这是错误的做法！EventLoop线程被阻塞了";
        });
    }

    /**
     * 演示5：正确处理阻塞操作
     * 
     * 访问：http://localhost:8080/comparison/correct-blocking
     * 
     * 使用subscribeOn切换到专门的线程池
     */
    @GetMapping("/correct-blocking")
    public Mono<String> correctBlocking() {
        return Mono.fromCallable(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("【正确】在独立线程池中执行阻塞操作！线程：" + threadName);
            
            // ✅ 正确：在独立线程池中执行
            Thread.sleep(1000);
            
            return "这是正确的做法！使用了boundedElastic线程池";
        }).subscribeOn(Schedulers.boundedElastic());  // 切换到弹性线程池
    }

    /**
     * 演示6：流式返回数据（Server-Sent Events）
     * 
     * 访问：http://localhost:8080/comparison/stream
     * 
     * 使用浏览器或curl访问：
     * curl http://localhost:8080/comparison/stream
     * 
     * 数据会实时推送给客户端
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream() {
        return Flux.interval(Duration.ofSeconds(1))  // 每秒生成一个数字
                .take(10)  // 总共10个
                .map(seq -> {
                    String threadName = Thread.currentThread().getName();
                    return String.format("[%d] 当前时间：%d | 线程：%s", 
                            seq, System.currentTimeMillis(), threadName);
                })
                .doOnComplete(() -> System.out.println("流式传输完成"));
    }

    /**
     * 演示7：错误处理
     * 
     * 访问：http://localhost:8080/comparison/error-handling
     * 
     * 展示如何优雅地处理错误
     */
    @GetMapping("/error-handling")
    public Mono<String> errorHandling() {
        return webClient.get()
                .uri("http://localhost:9999/not-exist")  // 不存在的服务
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(2))  // 2秒超时
                .doOnError(e -> System.err.println("发生错误：" + e.getClass().getSimpleName()))
                .onErrorResume(e -> {
                    // 发生错误时返回默认值
                    return Mono.just("服务不可用，返回默认值。错误：" + e.getMessage());
                });
    }

    /**
     * 演示8：线程使用情况监控
     * 
     * 访问：http://localhost:8080/comparison/thread-info
     * 
     * 显示当前线程信息
     */
    @GetMapping("/thread-info")
    public Mono<String> threadInfo() {
        String requestThread = Thread.currentThread().getName();
        
        return Mono.delay(Duration.ofMillis(100))
                .map(tick -> {
                    String responseThread = Thread.currentThread().getName();
                    int activeThreads = Thread.activeCount();
                    
                    return String.format(
                            "请求线程：%s\n响应线程：%s\n活跃线程数：%d\n\n" +
                            "说明：\n" +
                            "- WebFlux 使用少量线程处理大量请求\n" +
                            "- 请求和响应可能在不同线程中处理\n" +
                            "- 这就是异步非阻塞的威力！",
                            requestThread, responseThread, activeThreads
                    );
                });
    }

    /**
     * 演示9：对比测试汇总
     * 
     * 访问：http://localhost:8080/comparison/summary
     * 
     * 返回所有测试接口的说明
     */
    @GetMapping("/summary")
    public Mono<String> summary() {
        return Mono.just(
                "=== Spring WebFlux 演示接口 ===\n\n" +
                
                "1. 基础接口：\n" +
                "   GET /comparison/mock-service - 模拟外部服务（200ms延迟）\n\n" +
                
                "2. 单个请求：\n" +
                "   GET /comparison/webflux-single - 单个异步请求\n\n" +
                
                "3. 串行 vs 并行：\n" +
                "   GET /comparison/webflux-sequential - 串行调用（~600ms）\n" +
                "   GET /comparison/webflux-parallel - 并行调用（~200ms）★推荐测试\n\n" +
                
                "4. 阻塞处理：\n" +
                "   GET /comparison/wrong-blocking - 错误的阻塞方式\n" +
                "   GET /comparison/correct-blocking - 正确的阻塞方式\n\n" +
                
                "5. 流式数据：\n" +
                "   GET /comparison/stream - Server-Sent Events 实时推送\n\n" +
                
                "6. 错误处理：\n" +
                "   GET /comparison/error-handling - 优雅的错误处理\n\n" +
                
                "7. 线程信息：\n" +
                "   GET /comparison/thread-info - 查看线程使用情况\n\n" +
                
                "提示：\n" +
                "- 使用浏览器或Postman测试以上接口\n" +
                "- 观察控制台日志，注意线程名称的变化\n" +
                "- 重点测试 webflux-parallel，体验并行调用的威力！"
        );
    }
}

