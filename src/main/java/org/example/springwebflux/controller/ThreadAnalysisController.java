package org.example.springwebflux.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 线程分析控制器
 * 
 * 用于验证 WebFlux 等待机制的线程模型
 * 
 * 核心问题：
 * 1. A接口等待时，线程是否阻塞？
 * 2. B接口触发时，数据写入发生在哪个线程？
 */
@RestController
@RequestMapping("/thread-analysis")
public class ThreadAnalysisController {

    private static final Logger logger = LoggerFactory.getLogger(ThreadAnalysisController.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final ConcurrentHashMap<String, MonoSink<ThreadInfo>> sinks = new ConcurrentHashMap<>();
    
    // 用于收集线程信息
    private final ConcurrentHashMap<String, StringBuilder> threadLogs = new ConcurrentHashMap<>();

    /**
     * A接口：等待数据（打印详细的线程信息）
     * 
     * 测试步骤：
     * 1. curl http://localhost:8080/thread-analysis/wait/test123
     * 2. curl -X POST http://localhost:8080/thread-analysis/send/test123 \
     *      -H "Content-Type: text/plain" \
     *      -d "Hello"
     * 
     * 观察控制台输出的线程信息
     */
    @GetMapping("/wait/{id}")
    public Mono<ThreadInfo> waitForData(@PathVariable String id) {
        String currentThread = Thread.currentThread().getName();
        String time = LocalDateTime.now().format(TIME_FORMATTER);
        
        StringBuilder log = new StringBuilder();
//        log.append("\n" + "=".repeat(80) + "\n");
        log.append("【A接口】等待数据，ID=" + id + "\n");
//        log.append("=".repeat(80) + "\n");
        log.append(String.format("[%s] ① A接口方法入口 - 线程: %s\n", time, currentThread));
        
        threadLogs.put(id, log);
        
        return Mono.<ThreadInfo>create(sink -> {
            String createThread = Thread.currentThread().getName();
            String createTime = LocalDateTime.now().format(TIME_FORMATTER);
            
            log.append(String.format("[%s] ② Mono.create() lambda执行 - 线程: %s\n", createTime, createThread));
            log.append(String.format("[%s] ③ 将 MonoSink 存入 Map\n", createTime));
            
            // 存储 sink
            sinks.put(id, sink);
            
            log.append(String.format("[%s] ④ lambda执行完毕，即将返回\n", createTime));
            log.append(String.format("[%s] ⑤ 线程 %s 即将释放！\n", createTime, createThread));
//            log.append("-".repeat(80) + "\n");
            log.append("💡 注意：此时线程已经释放，不会阻塞等待！\n");
            log.append("💡 线程可以去处理其他请求了！\n");
//            log.append("-".repeat(80) + "\n\n");
            
            System.out.println(log.toString());
            
            // 清理
            sink.onDispose(() -> {
                sinks.remove(id);
                threadLogs.remove(id);
                logger.info("客户端断开，清理资源，ID={}", id);
            });
        })
        // 监听各个阶段的线程
        .doOnSubscribe(subscription -> {
            String subThread = Thread.currentThread().getName();
            String subTime = LocalDateTime.now().format(TIME_FORMATTER);
            log.append(String.format("[%s] ⚡ doOnSubscribe - 线程: %s\n", subTime, subThread));
        })
        .doOnRequest(n -> {
            String reqThread = Thread.currentThread().getName();
            String reqTime = LocalDateTime.now().format(TIME_FORMATTER);
            log.append(String.format("[%s] ⚡ doOnRequest - 线程: %s\n", reqTime, reqThread));
        })
        // 设置超时
        .timeout(Duration.ofSeconds(60))
        .onErrorResume(e -> {
            logger.error("等待超时，ID={}", id);
            sinks.remove(id);
            threadLogs.remove(id);
            return Mono.just(new ThreadInfo("超时", "无", "等待超时"));
        });
    }

    /**
     * B接口：发送数据（打印详细的线程信息）
     * 
     * 这个接口会触发 A 接口返回，并显示在哪个线程执行
     */
    @PostMapping("/send/{id}")
    public Mono<String> sendData(@PathVariable String id, @RequestBody String data) {
        String currentThread = Thread.currentThread().getName();
        String time = LocalDateTime.now().format(TIME_FORMATTER);
        
        StringBuilder log = threadLogs.get(id);
        if (log == null) {
            log = new StringBuilder();
        }
        
//        log.append("\n" + "=".repeat(80) + "\n");
        log.append("【B接口】发送数据，ID=" + id + "\n");
//        log.append("=".repeat(80) + "\n");
        log.append(String.format("[%s] ⑥ B接口方法入口 - 线程: %s\n", time, currentThread));
        
        MonoSink<ThreadInfo> sink = sinks.get(id);
        
        if (sink != null) {
            log.append(String.format("[%s] ⑦ 从 Map 中找到 MonoSink\n", time));
            log.append(String.format("[%s] ⑧ 准备调用 sink.success(data)\n", time));
            log.append(String.format("[%s] ⑨ 调用 sink.success() - 线程: %s\n", time, currentThread));
            
            // 创建返回数据
            ThreadInfo info = new ThreadInfo(
                    "A接口方法线程（已释放）",
                    currentThread,  // B接口的线程
                    "数据写入发生在 B接口的线程！"
            );
            
            // 触发 A 接口返回 - 在当前线程（B的线程）执行
            sink.success(info);
            
            log.append(String.format("[%s] ⑩ sink.success() 调用完成\n", time));
            log.append(String.format("[%s] ⑪ onNext 回调已触发（在当前线程）\n", time));
            log.append(String.format("[%s] ⑫ 数据已写入 HTTP 响应\n", time));
//            log.append("-".repeat(80) + "\n");
            log.append("🎯 关键结论：数据写入发生在 B接口的线程：" + currentThread + "\n");
            log.append("🎯 不是发生在 A接口的线程！\n");
//            log.append("-".repeat(80) + "\n");
            
            System.out.println(log.toString());
            
            sinks.remove(id);
            threadLogs.remove(id);
            
            return Mono.just("✅ 数据已发送，ID=" + id + "\n发送线程: " + currentThread);
        } else {
            log.append(String.format("[%s] ❌ 没有找到等待的 A 接口\n", time));
            System.out.println(log.toString());
            threadLogs.remove(id);
            return Mono.just("❌ 没有等待的 A 接口，ID=" + id);
        }
    }

    /**
     * 演示线程不阻塞
     * 
     * 测试步骤：
     * 1. 同时发起多个请求：
     *    curl http://localhost:8080/thread-analysis/demo/1 &
     *    curl http://localhost:8080/thread-analysis/demo/2 &
     *    curl http://localhost:8080/thread-analysis/demo/3 &
     * 
     * 2. 观察控制台：可能只用了很少的线程处理3个请求
     */
//    @GetMapping("/demo/{id}")
//    public Mono<String> demo(@PathVariable String id) {
//        String thread1 = Thread.currentThread().getName();
//
//        return Mono.create(sink -> {
//            String thread2 = Thread.currentThread().getName();
//            logger.info("ID={}, 入口线程={}, create线程={}", id, thread1, thread2);
//
//            // 模拟存储
//            sinks.put("demo-" + id, (MonoSink) sink);
//
//            // 2秒后自动触发（模拟B接口）
//            new Thread(() -> {
//                try {
//                    Thread.sleep(2000);
//                    String thread3 = Thread.currentThread().getName();
//                    logger.info("ID={}, 触发线程={}", id, thread3);
//                    sink.success("ID=" + id + ", 返回线程=" + thread3);
//                    sinks.remove("demo-" + id);
//                } catch (InterruptedException e) {
//                    sink.error(e);
//                }
//            }).start();
//        })
//        .doOnNext(result -> {
//            String thread4 = Thread.currentThread().getName();
//            logger.info("ID={}, doOnNext线程={}", id, thread4);
//        })
//        .timeout(Duration.ofSeconds(10));
//    }

    /**
     * 对比：传统阻塞方式（错误示范）
     * 
     * 注意：这会阻塞线程！
     */
//    @GetMapping("/blocking-demo/{id}")
//    public Mono<String> blockingDemo(@PathVariable String id) {
//        String thread1 = Thread.currentThread().getName();
//        logger.info("❌ 阻塞演示 ID={}, 线程={}", id, thread1);
//
//        return Mono.create(sink -> {
//            String thread2 = Thread.currentThread().getName();
//            logger.info("❌ 准备阻塞线程 {}", thread2);
//
//            try {
//                // ❌ 错误：阻塞等待
//                Thread.sleep(5000);  // 线程被阻塞 5 秒
//                logger.info("❌ 线程 {} 阻塞结束", thread2);
//            } catch (InterruptedException e) {
//                sink.error(e);
//                return;
//            }
//
//            sink.success("阻塞5秒后返回，线程=" + thread2);
//        })
//        .doOnNext(result -> {
//            String thread3 = Thread.currentThread().getName();
//            logger.info("❌ 返回线程={}", thread3);
//        });
//    }

    /**
     * 查看当前等待的请求
     */
    @GetMapping("/status")
    public Mono<String> status() {
        return Mono.just(String.format(
                "当前状态：\n" +
                "- 等待中的请求：%d 个\n" +
                "- ID列表：%s\n" +
                "- 当前线程：%s",
                sinks.size(),
                sinks.isEmpty() ? "无" : sinks.keySet(),
                Thread.currentThread().getName()
        ));
    }

    /**
     * 压力测试：同时创建多个等待请求
     * 
     * 测试：
     * for i in {1..100}; do curl http://localhost:8080/thread-analysis/stress/$i & done
     * 
     * 观察：使用的线程数远少于100个
     */
    @GetMapping("/stress/{id}")
    public Mono<String> stress(@PathVariable String id) {
        String currentThread = Thread.currentThread().getName();
        logger.info("压力测试 ID={}, 线程={}", id, currentThread);
        
        return Mono.<String>create(sink -> {
            sinks.put("stress-" + id, (MonoSink) sink);
            logger.info("ID={} 已注册，等待触发", id);
        })
        .timeout(Duration.ofSeconds(30))
        .onErrorReturn("超时");
    }

    /**
     * 触发所有压力测试请求
     */
    @PostMapping("/stress-trigger")
//    public Mono<String> stressTrigger(@RequestBody String data) {
//        String currentThread = Thread.currentThread().getName();
//        int count = 0;
//
//        // 触发所有 stress- 开头的请求
//        for (String key : sinks.keySet()) {
//            if (key.startsWith("stress-")) {
//                MonoSink<String> sink = (MonoSink<String>) sinks.get(key);
//                if (sink != null) {
//                    sink.success("触发线程=" + currentThread + ", 数据=" + data);
//                    count++;
//                }
//            }
//        }
//
//        // 清理
//        sinks.keySet().removeIf(k -> k.startsWith("stress-"));
//
//        return Mono.just(String.format(
//                "✅ 已触发 %d 个请求\n触发线程: %s",
//                count,
//                currentThread
//        ));
//    }

    /**
     * 使用指南
     */
    @GetMapping("/guide")
    public Mono<String> guide() {
        return Mono.just(
                "=== WebFlux 线程分析指南 ===\n\n" +
                
                "【核心测试】验证线程模型\n" +
                "1. 终端1：curl http://localhost:8080/thread-analysis/wait/test123\n" +
                "2. 终端2：curl -X POST http://localhost:8080/thread-analysis/send/test123 \\\n" +
                "            -H \"Content-Type: text/plain\" -d \"Hello\"\n" +
                "3. 观察控制台输出，验证：\n" +
                "   - A接口的线程是否释放\n" +
                "   - B接口在哪个线程执行\n" +
                "   - 数据写入在哪个线程\n\n" +
                
                "【演示线程复用】\n" +
                "curl http://localhost:8080/thread-analysis/demo/1 &\n" +
                "curl http://localhost:8080/thread-analysis/demo/2 &\n" +
                "curl http://localhost:8080/thread-analysis/demo/3 &\n\n" +
                
                "【对比阻塞方式】\n" +
                "curl http://localhost:8080/thread-analysis/blocking-demo/test\n" +
                "（会阻塞线程5秒）\n\n" +
                
                "【压力测试】\n" +
                "1. for i in {1..100}; do curl http://localhost:8080/thread-analysis/stress/$i & done\n" +
                "2. curl -X POST http://localhost:8080/thread-analysis/stress-trigger \\\n" +
                "     -H \"Content-Type: text/plain\" -d \"Trigger\"\n" +
                "3. 观察：100个请求使用的线程数 << 100\n\n" +
                
                "【查看状态】\n" +
                "curl http://localhost:8080/thread-analysis/status\n\n" +
                
                "关键观察点：\n" +
                "✅ A接口线程快速释放（不阻塞）\n" +
                "✅ B接口在不同的线程执行\n" +
                "✅ 数据写入发生在 B接口的线程\n" +
                "✅ 少量线程处理大量请求"
        );
    }

    // 辅助类：线程信息
    static class ThreadInfo {
        private String aThread;
        private String bThread;
        private String conclusion;

        public ThreadInfo(String aThread, String bThread, String conclusion) {
            this.aThread = aThread;
            this.bThread = bThread;
            this.conclusion = conclusion;
        }

        public String getaThread() {
            return aThread;
        }

        public String getbThread() {
            return bThread;
        }

        public String getConclusion() {
            return conclusion;
        }

        @Override
        public String toString() {
            return "ThreadInfo{" +
                    "A接口线程='" + aThread + '\'' +
                    ", B接口线程='" + bThread + '\'' +
                    ", 结论='" + conclusion + '\'' +
                    '}';
        }
    }
}

