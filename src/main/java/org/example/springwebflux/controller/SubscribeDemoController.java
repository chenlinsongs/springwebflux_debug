package org.example.springwebflux.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * 演示 WebFlux 的自动订阅机制
 * 
 * 展示为什么需要 subscribe 以及框架如何自动 subscribe
 */
@RestController
@RequestMapping("/subscribe-demo")
public class SubscribeDemoController {

    /**
     * 演示1：验证惰性执行
     * 
     * 访问：http://localhost:8080/subscribe-demo/lazy-execution
     * 
     * 观察控制台输出顺序
     */
    @GetMapping("/lazy-execution")
    public Mono<String> lazyExecution() {
        System.out.println("\n=== 惰性执行演示 ===");
        System.out.println(">>> Step 1: Controller 方法开始执行");
        
        Mono<String> mono = Mono.fromCallable(() -> {
            System.out.println(">>> Step 3: Mono 内部代码执行（只有被 subscribe 时才执行）");
            return "执行结果";
        });
        
        System.out.println(">>> Step 2: Mono 已创建，准备返回（还没执行内部代码）");
        
        return mono;
        
        // 框架会自动 subscribe，所以 Step 3 会执行
        // 如果没有 subscribe，Step 3 永远不会执行
    }

    /**
     * 演示2：对比有订阅 vs 无订阅
     * 
     * 访问：http://localhost:8080/subscribe-demo/with-without-subscribe
     */
    @GetMapping("/with-without-subscribe")
    public Mono<String> withWithoutSubscribe() {
        System.out.println("\n=== 对比有订阅 vs 无订阅 ===");
        
        // === 无订阅的情况 ===
        System.out.println("1. 创建 Mono（无订阅）");
        Mono<String> monoWithoutSubscribe = Mono.fromCallable(() -> {
            System.out.println("   这行代码不会执行（因为没有订阅）");
            return "不会输出";
        });
        System.out.println("2. Mono 创建完成（代码没执行）\n");
        
        // === 有订阅的情况 ===
        System.out.println("3. 创建 Mono（将被订阅）");
        Mono<String> monoWithSubscribe = Mono.fromCallable(() -> {
            System.out.println("   这行代码会执行（因为被 subscribe 了）");
            return "会输出";
        });
        System.out.println("4. Mono 创建完成，准备返回给框架");
        
        return monoWithSubscribe;
        // 框架会自动 subscribe，所以 monoWithSubscribe 会执行
        // monoWithoutSubscribe 从未被订阅，所以不会执行
    }

    /**
     * 演示3：手动 subscribe vs 框架自动 subscribe
     * 
     * 访问：http://localhost:8080/subscribe-demo/manual-vs-auto
     */
    @GetMapping("/manual-vs-auto")
    public Mono<String> manualVsAuto() {
        System.out.println("\n=== 手动 subscribe vs 自动 subscribe ===");
        
        // === 手动 subscribe ===
        System.out.println("1. 创建 Mono，手动 subscribe");
        Mono<String> mono1 = Mono.just("手动订阅的结果");
        mono1.subscribe(result -> {
            System.out.println("   手动 subscribe 的回调：" + result);
        });
        System.out.println("2. 手动 subscribe 完成\n");
        
        // === 框架自动 subscribe ===
        System.out.println("3. 创建 Mono，返回给框架（框架会自动 subscribe）");
        Mono<String> mono2 = Mono.just("框架自动订阅的结果");
        System.out.println("4. 返回 Mono 给框架");
        
        return mono2;
        // 框架会自动 subscribe，类似于：
        // mono2.subscribe(result -> writeToHttpResponse(result));
    }

    /**
     * 演示4：subscribe 触发整个链路执行
     * 
     * 访问：http://localhost:8080/subscribe-demo/chain-execution
     */
    @GetMapping("/chain-execution")
    public Mono<String> chainExecution() {
        System.out.println("\n=== subscribe 触发整个链路执行 ===");
        System.out.println("1. 开始构建 Mono 链");
        
        return Mono.just("开始")
                .doOnNext(s -> System.out.println("2. 第1步执行：" + s))
                .map(s -> s + " -> 步骤1")
                .doOnNext(s -> System.out.println("3. 第2步执行：" + s))
                .flatMap(s -> {
                    System.out.println("4. 第3步执行（flatMap）：" + s);
                    return Mono.just(s + " -> 步骤2");
                })
                .doOnNext(s -> System.out.println("5. 第4步执行：" + s))
                .map(s -> {
                    System.out.println("6. 最后一步执行：" + s);
                    return s + " -> 完成";
                })
                .doOnSubscribe(sub -> 
                    System.out.println("⭐ subscribe 被调用，整个链路开始执行")
                );
        
        // 观察输出顺序：
        // 先是 doOnSubscribe（subscribe 被调用）
        // 然后是其他步骤（按顺序执行）
    }

    /**
     * 演示5：模拟框架的 subscribe 过程
     * 
     * 访问：http://localhost:8080/subscribe-demo/simulate-framework
     */
    @GetMapping("/simulate-framework")
    public void simulateFramework() {
        System.out.println("\n=== 模拟框架的 subscribe 过程 ===");
        
        // 1. 模拟 Controller 返回 Mono
        System.out.println("1. Controller 返回 Mono");
        Mono<String> mono = getUserData();
        
        // 2. 模拟框架接收 Mono
        System.out.println("2. 框架接收 Mono");
        
        // 3. 模拟框架 subscribe
        System.out.println("3. 框架开始 subscribe\n");
        mono.subscribe(
            // onNext: 数据到达时
            data -> {
                System.out.println("   框架 onNext 回调：收到数据 = " + data);
                System.out.println("   框架将数据写入 HTTP 响应");
            },
            // onError: 发生错误时
            error -> {
                System.err.println("   框架 onError 回调：" + error.getMessage());
                System.err.println("   框架返回错误响应");
            },
            // onComplete: 完成时
            () -> {
                System.out.println("   框架 onComplete 回调：流完成");
                System.out.println("   框架关闭连接\n");
            }
        );
        
        System.out.println("4. subscribe 注册完成，方法返回");
        System.out.println("   （实际执行是异步的）");
    }
    
    private Mono<String> getUserData() {
        return Mono.fromCallable(() -> {
            System.out.println("   → 执行数据查询（subscribe 触发）");
            Thread.sleep(100);  // 模拟延迟
            return "用户数据";
        });
    }

    /**
     * 演示6：没有 subscribe 的后果
     * 
     * 访问：http://localhost:8080/subscribe-demo/no-subscribe-consequence
     */
    @GetMapping("/no-subscribe-consequence")
    public String noSubscribeConsequence() {
        System.out.println("\n=== 没有 subscribe 的后果 ===");
        System.out.println("1. 创建 Mono");
        
        Mono<String> mono = Mono.fromCallable(() -> {
            System.out.println("   这行永远不会执行");
            return "永远看不到的结果";
        });
        
        System.out.println("2. Mono 创建完成");
        System.out.println("3. 返回普通 String（不返回 Mono）");
        System.out.println("4. 因为没有人 subscribe，Mono 内部代码永远不会执行\n");
        
        return "已返回，但 Mono 从未执行";
        
        // 结果：Mono 内部的打印永远不会出现
    }

    /**
     * 演示7：多次 subscribe
     * 
     * 访问：http://localhost:8080/subscribe-demo/multiple-subscribe
     */
    @GetMapping("/multiple-subscribe")
    public String multipleSubscribe() {
        System.out.println("\n=== 多次 subscribe ===");
        System.out.println("1. 创建 Mono");
        
        Mono<String> mono = Mono.fromCallable(() -> {
            System.out.println("   → Mono 内部代码执行");
            return "结果";
        });
        
        System.out.println("2. 第1次 subscribe");
        mono.subscribe(result -> System.out.println("   第1次收到：" + result));
        
        System.out.println("3. 第2次 subscribe");
        mono.subscribe(result -> System.out.println("   第2次收到：" + result));
        
        System.out.println("4. 第3次 subscribe");
        mono.subscribe(result -> System.out.println("   第3次收到：" + result));
        
        System.out.println("5. 完成\n");
        
        return "观察：每次 subscribe 都会触发执行";
        
        // 关键发现：Mono 内部代码会执行3次（每次 subscribe 都执行）
    }

    /**
     * 演示8：subscribe 的时机
     * 
     * 访问：http://localhost:8080/subscribe-demo/subscribe-timing
     */
    @GetMapping("/subscribe-timing")
    public Mono<String> subscribeTiming() {
        System.out.println("\n=== subscribe 的时机 ===");
        
        long startTime = System.currentTimeMillis();
        System.out.println("1. Controller 方法开始，时间：" + startTime);
        
        Mono<String> mono = Mono.delay(Duration.ofSeconds(1))  // 延迟1秒
                .map(tick -> {
                    long executionTime = System.currentTimeMillis();
                    System.out.println("2. Mono 内部执行，时间：" + executionTime);
                    System.out.println("   距离方法开始：" + (executionTime - startTime) + "ms");
                    return "延迟执行的结果";
                })
                .doOnSubscribe(sub -> {
                    long subscribeTime = System.currentTimeMillis();
                    System.out.println("⭐ subscribe 被调用，时间：" + subscribeTime);
                    System.out.println("   距离方法开始：" + (subscribeTime - startTime) + "ms");
                });
        
        long returnTime = System.currentTimeMillis();
        System.out.println("3. Controller 方法返回，时间：" + returnTime);
        System.out.println("   距离方法开始：" + (returnTime - startTime) + "ms");
        System.out.println("   （方法立即返回，实际执行在 subscribe 后）\n");
        
        return mono;
        
        // 观察：方法立即返回（< 1ms），实际执行在1秒后
    }

    /**
     * 演示9：框架自动订阅的完整流程
     * 
     * 访问：http://localhost:8080/subscribe-demo/complete-flow
     */
    @GetMapping("/complete-flow")
    public Mono<String> completeFlow() {
        System.out.println("\n=== 完整流程演示 ===");
        
        return Mono.just("开始")
                .doOnSubscribe(sub -> 
                    System.out.println("1️⃣ 框架调用 subscribe")
                )
                .doOnNext(s -> 
                    System.out.println("2️⃣ 上游发射数据：" + s)
                )
                .map(s -> {
                    System.out.println("3️⃣ 执行 map 转换");
                    return s + " -> 转换";
                })
                .doOnNext(s -> 
                    System.out.println("4️⃣ map 结果：" + s)
                )
                .flatMap(s -> {
                    System.out.println("5️⃣ 执行 flatMap");
                    return Mono.just(s + " -> flatMap");
                })
                .doOnNext(s -> 
                    System.out.println("6️⃣ flatMap 结果：" + s)
                )
                .doOnSuccess(s -> 
                    System.out.println("7️⃣ 成功完成，框架写回响应：" + s)
                )
                .doOnTerminate(() -> 
                    System.out.println("8️⃣ 流终止，框架关闭连接\n")
                );
    }

    /**
     * 演示10：总结和说明
     * 
     * 访问：http://localhost:8080/subscribe-demo/summary
     */
    @GetMapping("/summary")
    public Mono<String> summary() {
        return Mono.just(
                "=== WebFlux 自动订阅机制总结 ===\n\n" +
                
                "1. 为什么需要 subscribe？\n" +
                "   - Mono/Flux 是惰性的（lazy）\n" +
                "   - 只有 subscribe 时才会真正执行\n" +
                "   - 没有 subscribe，代码永远不会运行\n\n" +
                
                "2. 框架做了什么？\n" +
                "   - Controller 返回 Mono\n" +
                "   - 框架自动 subscribe\n" +
                "   - 数据到达时写入 HTTP 响应\n\n" +
                
                "3. subscribe 的作用？\n" +
                "   - 触发 Mono/Flux 执行\n" +
                "   - 注册回调（onNext, onError, onComplete）\n" +
                "   - 连接生产者和消费者\n\n" +
                
                "4. 为什么这样设计？\n" +
                "   - 支持异步非阻塞\n" +
                "   - 线程不会等待，立即释放\n" +
                "   - 支持组合、取消、超时等操作\n\n" +
                
                "5. 测试接口：\n" +
                "   - /subscribe-demo/lazy-execution - 惰性执行\n" +
                "   - /subscribe-demo/with-without-subscribe - 对比\n" +
                "   - /subscribe-demo/chain-execution - 链路执行\n" +
                "   - /subscribe-demo/simulate-framework - 模拟框架\n" +
                "   - /subscribe-demo/complete-flow - 完整流程\n\n" +
                
                "关键理解：\n" +
                "✅ Mono/Flux = 定义\"要做什么\"\n" +
                "✅ subscribe = \"开始执行\"\n" +
                "✅ 框架负责自动 subscribe\n" +
                "✅ 你只需要返回 Mono/Flux"
        );
    }
}

