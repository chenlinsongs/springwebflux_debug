package org.example.springwebflux.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件桥梁控制器
 * 
 * 实现 A 接口等待 B 接口数据的场景
 * 
 * 场景说明：
 * 1. 客户端请求 A 接口 → A 接口等待，不返回
 * 2. 用 Postman 请求 B 接口发送数据 → B 接口接收数据
 * 3. A 接口收到数据 → 返回给客户端
 */
@RestController
@RequestMapping("/event-bridge")
public class EventBridgeController {

    private static final Logger logger = LoggerFactory.getLogger(EventBridgeController.class);

    // ============================
    // 方式1：简单版 - 一对一等待（带ID）
    // ============================
    
    /**
     * 存储等待的 MonoSink
     * Key: 请求ID
     * Value: MonoSink
     */
    private final ConcurrentHashMap<String, MonoSink<String>> waitingSinks = new ConcurrentHashMap<>();

    /**
     * A 接口：等待数据
     * 
     * 测试步骤：
     * 1. curl http://localhost:8080/event-bridge/wait/123
     *    （请求会挂起，等待数据）
     * 
     * 2. curl -X POST http://localhost:8080/event-bridge/send/123 \
     *      -H "Content-Type: text/plain" \
     *      -d "Hello from B"
     *    （发送数据）
     * 
     * 3. 步骤1的请求立即返回：Hello from B
     */
    @GetMapping("/wait/{id}")
    public Mono<String> waitForData(@PathVariable String id) {
        logger.info("\n=== A接口：等待数据，ID={} ===", id);
        
        return Mono.<String>create(sink -> {
            logger.info("创建 Mono，等待数据...");
            
            // 存储 sink 到 Map
            waitingSinks.put(id, sink);
            logger.info("已存储 sink 到 Map，当前等待数量：{}", waitingSinks.size());
            
            // 当客户端断开连接时清理
            sink.onDispose(() -> {
                waitingSinks.remove(id);
                logger.info("客户端断开连接，清理 sink，ID={}", id);
            });
        })
        // 设置超时：30秒后自动返回
        .timeout(Duration.ofSeconds(30))
        .doOnError(e -> {
            logger.error("等待超时或发生错误：{}", e.getMessage());
            waitingSinks.remove(id);
        })
        .onErrorReturn("等待超时（30秒），未收到数据")
        .doOnNext(data -> logger.info("A接口返回数据：{}", data));
    }

    /**
     * B 接口：发送数据
     * 
     * 当 B 接口收到数据后，会触发对应 ID 的 A 接口返回
     */
    @PostMapping("/send/{id}")
    public Mono<String> sendData(@PathVariable String id, @RequestBody String data) {
        logger.info("\n=== B接口：接收数据，ID={}, data={} ===", id, data);
        
        // 从 Map 中取出等待的 sink
        MonoSink<String> sink = waitingSinks.get(id);
        
        if (sink != null) {
            logger.info("找到等待的 A 接口，发送数据");
            // 发送数据给 A 接口
            sink.success(data);
            // 清理
            waitingSinks.remove(id);
            return Mono.just("✅ 数据已发送给 A 接口，ID=" + id);
        } else {
            logger.warn("没有找到等待的 A 接口，ID={}", id);
            return Mono.just("❌ 没有等待的 A 接口，ID=" + id + "（可能已超时或未发起请求）");
        }
    }

    // ============================
    // 方式2：多播版 - 多个A同时等待一个B
    // ============================
    
    /**
     * 多播 Sink：多个 A 接口可以同时订阅
     * B 接口发送一次数据，所有订阅的 A 接口都会收到
     */
    private final Sinks.Many<String> multicastSink = Sinks.many().multicast().onBackpressureBuffer();

    /**
     * A 接口：多播等待
     * 
     * 测试步骤：
     * 1. 终端1: curl http://localhost:8080/event-bridge/wait-multicast
     * 2. 终端2: curl http://localhost:8080/event-bridge/wait-multicast
     * 3. 终端3: curl http://localhost:8080/event-bridge/wait-multicast
     *    （三个请求都在等待）
     * 
     * 4. 终端4: curl -X POST http://localhost:8080/event-bridge/broadcast \
     *              -H "Content-Type: text/plain" \
     *              -d "Broadcast message"
     * 
     * 5. 终端1、2、3 同时收到：Broadcast message
     */
    @GetMapping("/wait-multicast")
    public Mono<String> waitMulticast() {
        logger.info("\n=== A接口：多播等待 ===");
        
        return multicastSink.asFlux()
                .next()  // 只取第一个数据
                .timeout(Duration.ofSeconds(60))
                .doOnSubscribe(sub -> logger.info("新的 A 接口开始等待"))
                .doOnNext(data -> logger.info("A接口收到广播数据：{}", data))
                .onErrorReturn("等待超时（60秒）");
    }

    /**
     * B 接口：广播数据
     * 
     * 发送数据给所有等待的 A 接口
     */
    @PostMapping("/broadcast")
    public Mono<String> broadcast(@RequestBody String data) {
        logger.info("\n=== B接口：广播数据，data={} ===", data);
        
        // 发送数据给所有订阅者
        Sinks.EmitResult result = multicastSink.tryEmitNext(data);
        
        if (result.isSuccess()) {
            logger.info("广播成功");
            return Mono.just("✅ 数据已广播给所有等待的 A 接口");
        } else {
            logger.warn("广播失败：{}", result);
            return Mono.just("❌ 广播失败：" + result);
        }
    }

    // ============================
    // 方式3：队列版 - 排队等待
    // ============================
    
    /**
     * 队列 Sink：多个 A 接口排队等待
     * B 接口每次发送数据，只有一个 A 接口收到
     */
    private final Sinks.Many<String> queueSink = Sinks.many().unicast().onBackpressureBuffer();

    /**
     * A 接口：排队等待
     * 
     * 测试步骤：
     * 1. 终端1: curl http://localhost:8080/event-bridge/wait-queue
     * 2. 终端2: curl http://localhost:8080/event-bridge/wait-queue
     * 3. 终端3: curl http://localhost:8080/event-bridge/wait-queue
     *    （三个请求都在等待）
     * 
     * 4. 终端4: curl -X POST http://localhost:8080/event-bridge/enqueue \
     *              -H "Content-Type: text/plain" \
     *              -d "Message 1"
     *    （只有终端1收到）
     * 
     * 5. 终端4: curl -X POST http://localhost:8080/event-bridge/enqueue \
     *              -H "Content-Type: text/plain" \
     *              -d "Message 2"
     *    （只有终端2收到）
     * 
     * 6. 终端4: curl -X POST http://localhost:8080/event-bridge/enqueue \
     *              -H "Content-Type: text/plain" \
     *              -d "Message 3"
     *    （只有终端3收到）
     */
    @GetMapping("/wait-queue")
    public Mono<String> waitQueue() {
        logger.info("\n=== A接口：排队等待 ===");
        
        return queueSink.asFlux()
                .next()  // 只取一个数据
                .timeout(Duration.ofSeconds(60))
                .doOnSubscribe(sub -> logger.info("新的 A 接口加入队列"))
                .doOnNext(data -> logger.info("A接口从队列收到数据：{}", data))
                .onErrorReturn("等待超时（60秒）");
    }

    /**
     * B 接口：入队数据
     * 
     * 发送数据给队列中的第一个 A 接口
     */
    @PostMapping("/enqueue")
    public Mono<String> enqueue(@RequestBody String data) {
        logger.info("\n=== B接口：入队数据，data={} ===", data);
        
        Sinks.EmitResult result = queueSink.tryEmitNext(data);
        
        if (result.isSuccess()) {
            logger.info("入队成功");
            return Mono.just("✅ 数据已发送给队列中的一个 A 接口");
        } else {
            logger.warn("入队失败：{}", result);
            return Mono.just("❌ 入队失败：" + result);
        }
    }

    // ============================
    // 方式4：重播版 - 缓存最后一个值
    // ============================
    
    /**
     * 重播 Sink：缓存最后一个值
     * 新的 A 接口可以立即获取最后一个值
     */
    private final Sinks.Many<String> replaySink = Sinks.many().replay().latest();

    /**
     * A 接口：立即获取或等待
     * 
     * 如果 B 接口已经发送过数据，立即返回
     * 如果 B 接口还没发送，等待第一个数据
     */
    @GetMapping("/wait-replay")
    public Mono<String> waitReplay() {
        logger.info("\n=== A接口：重播等待（可立即获取缓存值）===");
        
        return replaySink.asFlux()
                .next()
                .timeout(Duration.ofSeconds(60))
                .doOnSubscribe(sub -> logger.info("新的 A 接口订阅重播流"))
                .doOnNext(data -> logger.info("A接口收到数据（可能是缓存）：{}", data))
                .onErrorReturn("等待超时（60秒）");
    }

    /**
     * B 接口：发送数据（带缓存）
     */
    @PostMapping("/replay")
    public Mono<String> replay(@RequestBody String data) {
        logger.info("\n=== B接口：发送数据（带缓存），data={} ===", data);
        
        Sinks.EmitResult result = replaySink.tryEmitNext(data);
        
        if (result.isSuccess()) {
            logger.info("发送成功，数据已缓存");
            return Mono.just("✅ 数据已发送并缓存");
        } else {
            logger.warn("发送失败：{}", result);
            return Mono.just("❌ 发送失败：" + result);
        }
    }

    // ============================
    // 辅助接口
    // ============================

    /**
     * 查看当前等待的 A 接口数量
     */
    @GetMapping("/status")
    public Mono<String> status() {
        int waitingCount = waitingSinks.size();
        return Mono.just(String.format(
                "当前状态：\n" +
                "- 等待中的 A 接口（带ID）：%d 个\n" +
                "- 等待 ID 列表：%s\n",
                waitingCount,
                waitingCount > 0 ? waitingSinks.keySet() : "无"
        ));
    }

    /**
     * 总结和使用指南
     */
    @GetMapping("/guide")
    public Mono<String> guide() {
        return Mono.just(
                "=== A接口等待B接口数据 - 使用指南 ===\n\n" +
                
                "【方式1：带ID的一对一等待】\n" +
                "1. A接口：GET  /event-bridge/wait/{id}  - 等待指定ID的数据\n" +
                "2. B接口：POST /event-bridge/send/{id}  - 发送数据给指定ID\n" +
                "3. 特点：精确匹配，一对一\n\n" +
                
                "【方式2：多播 - 一对多】\n" +
                "1. A接口：GET  /event-bridge/wait-multicast  - 等待广播\n" +
                "2. B接口：POST /event-bridge/broadcast      - 广播给所有等待的A\n" +
                "3. 特点：一次发送，所有A同时收到\n\n" +
                
                "【方式3：队列 - 排队处理】\n" +
                "1. A接口：GET  /event-bridge/wait-queue  - 加入队列等待\n" +
                "2. B接口：POST /event-bridge/enqueue     - 发送给队列中的一个A\n" +
                "3. 特点：先进先出，逐个处理\n\n" +
                
                "【方式4：重播 - 缓存最后值】\n" +
                "1. A接口：GET  /event-bridge/wait-replay  - 获取缓存或等待\n" +
                "2. B接口：POST /event-bridge/replay       - 发送并缓存\n" +
                "3. 特点：新的A可以立即获取上一次的值\n\n" +
                
                "【辅助接口】\n" +
                "- GET /event-bridge/status  - 查看等待状态\n" +
                "- GET /event-bridge/guide   - 查看本指南\n\n" +
                
                "【测试建议】\n" +
                "推荐顺序：方式1 → 方式2 → 方式3 → 方式4\n" +
                "详细测试步骤见文档：A接口等待B接口数据.md"
        );
    }

    /**
     * 测试示例接口
     */
    @GetMapping("/test-example")
    public Mono<String> testExample() {
        return Mono.just(
                "=== 快速测试示例 ===\n\n" +
                
                "【方式1测试】\n" +
                "# 终端1：A接口等待\n" +
                "curl http://localhost:8080/event-bridge/wait/123\n\n" +
                
                "# 终端2：B接口发送\n" +
                "curl -X POST http://localhost:8080/event-bridge/send/123 \\\n" +
                "  -H \"Content-Type: text/plain\" \\\n" +
                "  -d \"Hello from B\"\n\n" +
                
                "【方式2测试】\n" +
                "# 终端1-3：三个A接口同时等待\n" +
                "curl http://localhost:8080/event-bridge/wait-multicast\n" +
                "curl http://localhost:8080/event-bridge/wait-multicast\n" +
                "curl http://localhost:8080/event-bridge/wait-multicast\n\n" +
                
                "# 终端4：B接口广播\n" +
                "curl -X POST http://localhost:8080/event-bridge/broadcast \\\n" +
                "  -H \"Content-Type: text/plain\" \\\n" +
                "  -d \"Broadcast to all\"\n\n" +
                
                "所有终端1-3同时收到数据！"
        );
    }
}

