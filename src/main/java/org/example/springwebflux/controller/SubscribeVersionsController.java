package org.example.springwebflux.controller;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 演示 subscribe 方法的所有重载版本
 * 
 * 展示从简单到复杂的不同 subscribe 用法
 */
@RestController
@RequestMapping("/subscribe-versions")
public class SubscribeVersionsController {

    /**
     * 演示1：subscribe() - 无参数版本
     * 
     * 访问：http://localhost:8080/subscribe-versions/no-param
     */
    @GetMapping("/no-param")
    public String noParam() {
        System.out.println("\n=== 版本1：subscribe() 无参数 ===");
        
        Mono<String> mono = Mono.fromCallable(() -> {
            System.out.println(">>> Mono 执行");
            return "Hello";
        });
        
        // 版本1：无参数 - 只触发执行，不处理任何信号
        mono.subscribe();
        
        System.out.println("subscribe() 完成\n");
        
        return "查看控制台输出";
    }

    /**
     * 演示2：subscribe(Consumer) - 只处理数据
     * 
     * 访问：http://localhost:8080/subscribe-versions/with-consumer
     */
    @GetMapping("/with-consumer")
    public String withConsumer() {
        System.out.println("\n=== 版本2：subscribe(Consumer) ===");
        
        Flux<Integer> flux = Flux.range(1, 5);
        
        // 版本2：只处理数据
        flux.subscribe(data -> {
            System.out.println("收到数据：" + data);
        });
        
        System.out.println("subscribe(Consumer) 完成\n");
        
        return "查看控制台输出";
    }

    /**
     * 演示3：subscribe(Consumer, Consumer) - 处理数据+错误
     * 
     * 访问：http://localhost:8080/subscribe-versions/with-error-handler
     */
    @GetMapping("/with-error-handler")
    public String withErrorHandler() {
        System.out.println("\n=== 版本3：subscribe(Consumer, Consumer) ===");
        
        // 正常情况
        System.out.println("--- 正常情况 ---");
        Mono<String> monoSuccess = Mono.just("Success");
        monoSuccess.subscribe(
            data -> System.out.println("✅ 成功：" + data),
            error -> System.err.println("❌ 错误：" + error.getMessage())
        );
        
        // 错误情况
        System.out.println("\n--- 错误情况 ---");
        Mono<String> monoError = Mono.error(new RuntimeException("模拟错误"));
        monoError.subscribe(
            data -> System.out.println("✅ 成功：" + data),
            error -> System.err.println("❌ 错误：" + error.getMessage())
        );
        
        System.out.println("\nsubscribe(Consumer, Consumer) 完成\n");
        
        return "查看控制台输出";
    }

    /**
     * 演示4：subscribe(Consumer, Consumer, Runnable) - 处理数据+错误+完成
     * 
     * 访问：http://localhost:8080/subscribe-versions/with-complete
     */
    @GetMapping("/with-complete")
    public String withComplete() {
        System.out.println("\n=== 版本4：subscribe(Consumer, Consumer, Runnable) ===");
        
        Flux<Integer> flux = Flux.range(1, 5);
        
        // 版本4：处理数据 + 错误 + 完成
        flux.subscribe(
            data -> {
                System.out.println("📦 数据：" + data);
            },
            error -> {
                System.err.println("❌ 错误：" + error.getMessage());
            },
            () -> {
                System.out.println("✅ 完成！");
            }
        );
        
        System.out.println("\nsubscribe(..., Runnable) 完成\n");
        
        return "查看控制台输出";
    }

    /**
     * 演示5：subscribe(..., Consumer<Subscription>) - 处理数据+错误+完成+背压
     * 
     * 访问：http://localhost:8080/subscribe-versions/with-backpressure
     */
    @GetMapping("/with-backpressure")
    public String withBackpressure() {
        System.out.println("\n=== 版本5：subscribe(..., Consumer<Subscription>) ===");
        
        Flux<Integer> flux = Flux.range(1, 100);
        
        // 版本5：处理数据 + 错误 + 完成 + 背压控制
        flux.subscribe(
            data -> {
                System.out.println("📦 数据：" + data);
            },
            error -> {
                System.err.println("❌ 错误：" + error.getMessage());
            },
            () -> {
                System.out.println("✅ 完成！");
            },
            subscription -> {
                System.out.println("🔔 订阅时：只请求前10个数据");
                subscription.request(10);  // 只请求10个数据
            }
        );
        
        System.out.println("\nsubscribe(..., Consumer<Subscription>) 完成\n");
        
        return "查看控制台输出（只有前10个数据）";
    }

    /**
     * 演示6：subscribe(Subscriber) - 完整的 Subscriber 接口
     * 
     * 访问：http://localhost:8080/subscribe-versions/with-subscriber
     */
    @GetMapping("/with-subscriber")
    public String withSubscriber() {
        System.out.println("\n=== 版本6：subscribe(Subscriber) ===");
        
        Flux<Integer> flux = Flux.range(1, 10);
        
        // 版本6：使用完整的 Subscriber 接口
        flux.subscribe(new Subscriber<Integer>() {
            private Subscription subscription;
            private int count = 0;
            
            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                System.out.println("🔔 onSubscribe：订阅成功");
                s.request(3);  // 先请求3个
            }
            
            @Override
            public void onNext(Integer data) {
                count++;
                System.out.println("📦 onNext：收到第" + count + "个数据 = " + data);
                
                if (count % 3 == 0) {
                    System.out.println("   → 请求下一批数据");
                    subscription.request(3);  // 每3个请求一次
                }
            }
            
            @Override
            public void onError(Throwable t) {
                System.err.println("❌ onError：" + t.getMessage());
            }
            
            @Override
            public void onComplete() {
                System.out.println("✅ onComplete：总共收到" + count + "个数据");
            }
        });
        
        System.out.println("\nsubscribe(Subscriber) 完成\n");
        
        return "查看控制台输出";
    }

    /**
     * 演示7：对比所有版本
     * 
     * 访问：http://localhost:8080/subscribe-versions/comparison
     */
    @GetMapping("/comparison")
    public String comparison() {
        System.out.println("\n=== 对比所有 subscribe 版本 ===\n");
        
        // 版本1：无参数
        System.out.println("1️⃣ subscribe()");
        Mono.just("A").subscribe();
        System.out.println("   特点：只触发执行，不处理任何信号\n");
        
        // 版本2：Consumer
        System.out.println("2️⃣ subscribe(Consumer)");
        Mono.just("B").subscribe(data -> 
            System.out.println("   收到数据：" + data)
        );
        System.out.println("   特点：只处理数据\n");
        
        // 版本3：Consumer + Consumer
        System.out.println("3️⃣ subscribe(Consumer, Consumer)");
        Mono.just("C").subscribe(
            data -> System.out.println("   收到数据：" + data),
            error -> System.err.println("   错误：" + error)
        );
        System.out.println("   特点：处理数据 + 错误\n");
        
        // 版本4：Consumer + Consumer + Runnable
        System.out.println("4️⃣ subscribe(Consumer, Consumer, Runnable)");
        Flux.just("D", "E").subscribe(
            data -> System.out.println("   收到数据：" + data),
            error -> System.err.println("   错误：" + error),
            () -> System.out.println("   完成")
        );
        System.out.println("   特点：处理数据 + 错误 + 完成\n");
        
        // 版本5：Consumer + Consumer + Runnable + Consumer
        System.out.println("5️⃣ subscribe(..., Consumer<Subscription>)");
        Flux.range(1, 5).subscribe(
            data -> System.out.println("   收到数据：" + data),
            error -> System.err.println("   错误：" + error),
            () -> System.out.println("   完成"),
            sub -> {
                System.out.println("   订阅时：请求3个数据");
                sub.request(3);
            }
        );
        System.out.println("   特点：处理数据 + 错误 + 完成 + 背压\n");
        
        // 版本6：Subscriber
        System.out.println("6️⃣ subscribe(Subscriber)");
        Mono.just("F").subscribe(new Subscriber<String>() {
            @Override
            public void onSubscribe(Subscription s) {
                System.out.println("   onSubscribe");
                s.request(1);
            }
            
            @Override
            public void onNext(String data) {
                System.out.println("   onNext：" + data);
            }
            
            @Override
            public void onError(Throwable t) {
                System.err.println("   onError");
            }
            
            @Override
            public void onComplete() {
                System.out.println("   onComplete");
            }
        });
        System.out.println("   特点：完全自定义，最灵活\n");
        
        return "查看控制台输出";
    }

    /**
     * 演示8：实际应用场景
     * 
     * 访问：http://localhost:8080/subscribe-versions/real-world
     */
    @GetMapping("/real-world")
    public String realWorld() {
        System.out.println("\n=== 实际应用场景 ===\n");
        
        // 场景1：只需要打印日志
        System.out.println("场景1：只需要打印日志");
        Mono.just("用户数据")
            .subscribe(data -> System.out.println("  日志：" + data));
        
        // 场景2：HTTP 调用（需要处理错误）
        System.out.println("\n场景2：HTTP 调用");
        simulateHttpCall()
            .subscribe(
                data -> System.out.println("  成功：" + data),
                error -> System.err.println("  失败：" + error.getMessage())
            );
        
        // 场景3：处理流（需要知道完成）
        System.out.println("\n场景3：处理数据流");
        Flux.range(1, 3)
            .subscribe(
                data -> System.out.println("  处理：" + data),
                error -> System.err.println("  错误：" + error),
                () -> System.out.println("  所有数据处理完成")
            );
        
        // 场景4：大量数据（需要背压）
        System.out.println("\n场景4：大量数据（背压控制）");
        Flux.range(1, 1000)
            .subscribe(
                data -> {
                    if (data <= 5) {  // 只打印前5个
                        System.out.println("  处理：" + data);
                    }
                },
                error -> System.err.println("  错误：" + error),
                () -> System.out.println("  完成"),
                sub -> sub.request(100)  // 一次请求100个
            );
        
        System.out.println();
        
        return "查看控制台输出";
    }

    /**
     * 总结
     */
    @GetMapping("/summary")
    public Mono<String> summary() {
        return Mono.just(
                "=== subscribe 方法重载版本总结 ===\n\n" +
                
                "1️⃣ subscribe()\n" +
                "   - 用途：只触发执行\n" +
                "   - 场景：不关心结果的副作用\n\n" +
                
                "2️⃣ subscribe(Consumer)\n" +
                "   - 用途：只处理数据\n" +
                "   - 场景：打印日志、简单处理\n" +
                "   - 最常用 ⭐\n\n" +
                
                "3️⃣ subscribe(Consumer, Consumer)\n" +
                "   - 用途：处理数据 + 错误\n" +
                "   - 场景：HTTP调用、数据库查询\n" +
                "   - 很常用 ⭐\n\n" +
                
                "4️⃣ subscribe(Consumer, Consumer, Runnable)\n" +
                "   - 用途：处理数据 + 错误 + 完成\n" +
                "   - 场景：处理流、需要知道完成\n" +
                "   - 常用\n\n" +
                
                "5️⃣ subscribe(..., Consumer<Subscription>)\n" +
                "   - 用途：处理数据 + 错误 + 完成 + 背压\n" +
                "   - 场景：大量数据、需要流量控制\n" +
                "   - 特殊场景\n\n" +
                
                "6️⃣ subscribe(Subscriber)\n" +
                "   - 用途：完全自定义\n" +
                "   - 场景：框架开发、高级应用\n" +
                "   - 很少用\n\n" +
                
                "选择原则：够用就好！\n" +
                "- 只要数据 → Consumer\n" +
                "- 要处理错误 → 两个 Consumer\n" +
                "- 要知道完成 → 三个参数\n" +
                "- 要背压控制 → 四个参数\n" +
                "- 要完全自定义 → Subscriber\n\n" +
                
                "测试接口：\n" +
                "- /subscribe-versions/no-param\n" +
                "- /subscribe-versions/with-consumer\n" +
                "- /subscribe-versions/with-error-handler\n" +
                "- /subscribe-versions/with-complete\n" +
                "- /subscribe-versions/with-backpressure\n" +
                "- /subscribe-versions/with-subscriber\n" +
                "- /subscribe-versions/comparison\n" +
                "- /subscribe-versions/real-world"
        );
    }

    // 辅助方法：模拟 HTTP 调用
    private Mono<String> simulateHttpCall() {
        return Mono.just("API响应数据");
    }
}

