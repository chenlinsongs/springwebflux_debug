package org.example.springwebflux.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * 线程模型演示
 * 
 * 展示 flatMap 的异步原理和线程切换
 */
@RestController
@RequestMapping("/thread-demo")
public class ThreadDemoController {

    @Autowired
    private WebClient webClient;

    /**
     * 演示1：观察 flatMap 中的线程
     * 
     * 访问：http://localhost:8080/thread-demo/basic
     * 
     * 观察控制台输出，看线程名称的变化
     */
    @GetMapping("/basic")
    public Mono<String> basicThreadDemo() {
        log("=== 开始演示 ===");
        log("0. Controller 方法开始");
        
        return Mono.just(1)
                .doOnNext(i -> log("1. Mono.just 发射数据"))
                
                .flatMap(id -> {
                    log("2. 进入 flatMap 函数");
                    
                    // 创建并返回新的 Mono（WebClient 调用）
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnSubscribe(sub -> log("3. 内部 Mono 被订阅"))
                            .doOnNext(result -> log("4. HTTP 响应到达，数据=" + result));
                })
                
                .map(result -> {
                    log("5. 最后的 map 处理");
                    return "最终结果：" + result;
                })
                
                .doOnSuccess(result -> log("6. 完成，准备返回"));
    }

    /**
     * 演示2：没有异步操作的情况
     * 
     * 访问：http://localhost:8080/thread-demo/no-async
     * 
     * 所有操作在同一个线程中执行
     */
    @GetMapping("/no-async")
    public Mono<String> noAsyncDemo() {
        log("=== 没有异步操作 ===");
        
        return Mono.just(1)
                .doOnNext(i -> log("步骤1：发射数据"))
                
                .flatMap(id -> {
                    log("步骤2：flatMap 函数");
                    // 没有异步操作，直接返回 Mono.just
                    return Mono.just("User-" + id);
                })
                
                .map(user -> {
                    log("步骤3：map 转换");
                    return "结果：" + user;
                })
                
                .doOnSuccess(result -> log("步骤4：完成"));
        
        // 结果：所有步骤都在同一个线程（reactor-http-nio-X）
    }

    /**
     * 演示3：对比同步阻塞 vs 异步非阻塞
     * 
     * 访问：http://localhost:8080/thread-demo/blocking-vs-nonblocking
     */
    @GetMapping("/blocking-vs-nonblocking")
    public Mono<String> blockingVsNonBlockingDemo() {
        log("=== 对比演示 ===");
        
        long start = System.currentTimeMillis();
        log("开始时间：" + start);
        
        return Mono.just(1)
                .flatMap(id -> {
                    log("发起第1个请求");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class);
                })
                .flatMap(result1 -> {
                    log("第1个请求完成，发起第2个请求");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .map(result2 -> result1 + " -> " + result2);
                })
                .map(finalResult -> {
                    long end = System.currentTimeMillis();
                    log("结束时间：" + end);
                    return String.format("耗时：%dms\n结果：%s\n\n说明：两个请求串行执行，但线程没有阻塞等待", 
                            (end - start), finalResult);
                });
    }

    /**
     * 演示4：使用 subscribeOn 指定执行线程
     * 
     * 访问：http://localhost:8080/thread-demo/subscribe-on
     */
    @GetMapping("/subscribe-on")
    public Mono<String> subscribeOnDemo() {
        log("=== subscribeOn 演示 ===");
        
        return Mono.just(1)
                .doOnNext(i -> log("subscribeOn 之前"))
                
                .subscribeOn(Schedulers.boundedElastic())  // 指定在 boundedElastic 线程池执行
                
                .doOnNext(i -> log("subscribeOn 之后"))
                
                .flatMap(id -> {
                    log("flatMap 函数");
                    return Mono.just("User-" + id);
                })
                
                .map(result -> {
                    log("map 函数");
                    return "结果：" + result;
                });
        
        // 结果：所有操作都在 boundedElastic 线程池中执行
    }

    /**
     * 演示5：使用 publishOn 切换线程
     * 
     * 访问：http://localhost:8080/thread-demo/publish-on
     */
    @GetMapping("/publish-on")
    public Mono<String> publishOnDemo() {
        log("=== publishOn 演示 ===");
        
        return Mono.just(1)
                .doOnNext(i -> log("publishOn 之前"))
                
                .publishOn(Schedulers.parallel())  // 从这里开始切换到 parallel 线程池
                
                .doOnNext(i -> log("publishOn 之后"))
                
                .flatMap(id -> {
                    log("flatMap 函数");
                    return Mono.just("User-" + id);
                })
                
                .map(result -> {
                    log("map 函数");
                    return "结果：" + result;
                });
        
        // 结果：
        // - publishOn 之前：在 reactor-http-nio-X
        // - publishOn 之后：在 parallel-X
    }

    /**
     * 演示6：多个 flatMap 的线程行为
     * 
     * 访问：http://localhost:8080/thread-demo/multiple-flatmap
     */
    @GetMapping("/multiple-flatmap")
    public Mono<String> multipleFlatMapDemo() {
        log("=== 多个 flatMap 演示 ===");
        
        return Mono.just(1)
                .doOnNext(i -> log("起点"))
                
                .flatMap(id -> {
                    log("第1个 flatMap");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnNext(result -> log("第1个响应到达"));
                })
                
                .flatMap(result1 -> {
                    log("第2个 flatMap");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnNext(result -> log("第2个响应到达"));
                })
                
                .flatMap(result2 -> {
                    log("第3个 flatMap");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnNext(result -> log("第3个响应到达"));
                })
                
                .map(finalResult -> {
                    log("最终处理");
                    return "完成：" + finalResult;
                });
        
        // 观察：每个 HTTP 响应到达时，线程可能都不同
    }

    /**
     * 演示7：Flux 的 flatMap（并发执行）
     * 
     * 访问：http://localhost:8080/thread-demo/flux-concurrent
     */
    @GetMapping("/flux-concurrent")
    public Flux<String> fluxConcurrentDemo() {
        log("=== Flux 并发演示 ===");
        
        return Flux.range(1, 5)  // 创建5个数字
                .doOnNext(i -> log("发射数字：" + i))
                
                .flatMap(id -> {
                    log("flatMap 处理：" + id);
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnNext(result -> log("响应" + id + "到达"))
                            .map(result -> "结果" + id + ": " + result);
                }, 5)  // 并发度=5，5个请求同时发起
                
                .doOnComplete(() -> log("所有请求完成"));
        
        // 关键：5个 HTTP 请求会并发执行，响应可能在不同的线程中到达
    }

    /**
     * 演示8：信号传递过程
     * 
     * 访问：http://localhost:8080/thread-demo/signals
     */
    @GetMapping("/signals")
    public Mono<String> signalsDemo() {
        log("=== 信号传递演示 ===");
        
        return Mono.just(1)
                .doOnSubscribe(sub -> log("信号：onSubscribe - 建立订阅关系"))
                
                .doOnNext(i -> log("信号：onNext - 收到数据 " + i))
                
                .flatMap(id -> {
                    log("flatMap 开始");
                    return webClient.get()
                            .uri("http://localhost:8080/comparison/mock-service")
                            .retrieve()
                            .bodyToMono(String.class)
                            .doOnSubscribe(sub -> log("  内部 Mono - 信号：onSubscribe"))
                            .doOnNext(result -> log("  内部 Mono - 信号：onNext"))
                            .doOnSuccess(result -> log("  内部 Mono - 信号：onSuccess"))
                            .doOnTerminate(() -> log("  内部 Mono - 信号：onTerminate"));
                })
                
                .doOnNext(result -> log("信号：onNext - 收到 flatMap 结果"))
                
                .doOnSuccess(result -> log("信号：onSuccess - 流完成"))
                
                .doOnTerminate(() -> log("信号：onTerminate - 终止"))
                
                .map(result -> "完成");
    }

    /**
     * 演示9：线程池对比
     * 
     * 访问：http://localhost:8080/thread-demo/schedulers
     */
    @GetMapping("/schedulers")
    public Flux<String> schedulersDemo() {
        log("=== 线程池对比演示 ===");
        
        return Flux.just(
                "immediate - 当前线程",
                "single - 单线程",
                "parallel - 并行线程池",
                "boundedElastic - 弹性线程池"
        )
        .flatMap(name -> {
            if (name.contains("immediate")) {
                return Mono.just(name)
                        .subscribeOn(Schedulers.immediate())
                        .map(n -> n + " → 线程：" + Thread.currentThread().getName());
            } else if (name.contains("single")) {
                return Mono.just(name)
                        .subscribeOn(Schedulers.single())
                        .map(n -> n + " → 线程：" + Thread.currentThread().getName());
            } else if (name.contains("parallel")) {
                return Mono.just(name)
                        .subscribeOn(Schedulers.parallel())
                        .map(n -> n + " → 线程：" + Thread.currentThread().getName());
            } else {
                return Mono.just(name)
                        .subscribeOn(Schedulers.boundedElastic())
                        .map(n -> n + " → 线程：" + Thread.currentThread().getName());
            }
        });
    }

    /**
     * 演示10：错误示范 - 在 flatMap 中阻塞（不要这样做）
     * 
     * 访问：http://localhost:8080/thread-demo/wrong-blocking
     */
    @GetMapping("/wrong-blocking")
    public Mono<String> wrongBlockingDemo() {
        log("=== 错误示范：阻塞线程 ===");
        
        return Mono.just(1)
                .flatMap(id -> {
                    log("⚠️ 警告：即将阻塞线程");
                    
                    // ❌ 错误：在 EventLoop 线程中阻塞
                    try {
                        Thread.sleep(1000);  // 阻塞 1 秒
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    log("⚠️ 阻塞结束");
                    return Mono.just("结果");
                })
                .map(result -> "这是错误的做法！EventLoop 线程被阻塞了 1 秒");
    }

    /**
     * 演示11：正确处理阻塞操作
     * 
     * 访问：http://localhost:8080/thread-demo/correct-blocking
     */
    @GetMapping("/correct-blocking")
    public Mono<String> correctBlockingDemo() {
        log("=== 正确处理阻塞操作 ===");
        
        return Mono.just(1)
                .flatMap(id -> {
                    log("准备执行阻塞操作");
                    
                    // ✅ 正确：在独立线程池中执行阻塞操作
                    return Mono.fromCallable(() -> {
                        log("在独立线程池中执行阻塞操作");
                        Thread.sleep(1000);
                        log("阻塞操作完成");
                        return "结果";
                    }).subscribeOn(Schedulers.boundedElastic());  // 在 boundedElastic 线程池执行
                })
                .map(result -> "这是正确的做法！使用了 boundedElastic 线程池");
    }

    /**
     * 演示12：总结和对比
     * 
     * 访问：http://localhost:8080/thread-demo/summary
     */
    @GetMapping("/summary")
    public Mono<String> summary() {
        return Mono.just(
                "=== flatMap 异步原理总结 ===\n\n" +
                
                "1. flatMap 不是异步的来源\n" +
                "   - flatMap 只是一个操作符\n" +
                "   - 异步来自 WebClient、R2DBC 等\n\n" +
                
                "2. 不一定在另一个线程执行\n" +
                "   - 没有异步操作：同一个线程\n" +
                "   - 有异步操作：可能切换线程\n\n" +
                
                "3. 线程交互通过信号机制\n" +
                "   - onSubscribe：订阅\n" +
                "   - onNext：数据到达\n" +
                "   - onComplete：完成\n" +
                "   - onError：错误\n\n" +
                
                "4. 线程切换发生在\n" +
                "   - 异步操作边界（如 HTTP 响应到达）\n" +
                "   - subscribeOn/publishOn\n\n" +
                
                "5. 测试接口\n" +
                "   - /thread-demo/basic - 基础演示\n" +
                "   - /thread-demo/no-async - 无异步操作\n" +
                "   - /thread-demo/blocking-vs-nonblocking - 对比\n" +
                "   - /thread-demo/multiple-flatmap - 多个 flatMap\n" +
                "   - /thread-demo/flux-concurrent - 并发执行\n" +
                "   - /thread-demo/signals - 信号传递\n" +
                "   - /thread-demo/schedulers - 线程池对比\n\n" +
                
                "观察要点：\n" +
                "- 查看控制台日志\n" +
                "- 注意线程名称的变化\n" +
                "- 理解何时切换线程\n" +
                "- 理解信号如何传递"
        );
    }

    /**
     * 辅助方法：打印日志，包含线程信息
     */
    private void log(String message) {
        System.out.println(String.format("[%s] %s - 线程：%s",
                System.currentTimeMillis() % 10000,  // 简化的时间戳
                message,
                Thread.currentThread().getName()));
    }
}

