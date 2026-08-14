package org.example.springwebflux.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Spring WebFlux 异步工作模式演示
 * 
 * 这个Controller展示了Spring WebFlux与传统Servlet的区别
 */
@RestController
@RequestMapping("/example")
public class WebFluxExampleController {

    @Autowired
    private WebClient webClient;

    // ============================================================
    // 问题1：Spring WebFlux 如何返回数据？
    // ============================================================

    /**
     * 示例1：最简单的返回方式
     * 
     * 在Servlet中：
     *   @GetMapping("/hello")
     *   public String hello() {
     *       return "Hello";  // 阻塞，等待方法执行完毕
     *   }
     * 
     * 在WebFlux中：
     *   返回 Mono<String> 或 Flux<String>
     *   这是一个"承诺"（Promise），表示"我将来会给你一个String"
     */
    @GetMapping("/simple")
    public Mono<String> simple() {
        // 立即返回一个Mono，不会阻塞线程
        return Mono.just("Hello WebFlux");
    }

    /**
     * 示例2：带有业务逻辑的返回
     * 
     * 在Servlet中：
     *   @GetMapping("/process/{id}")
     *   public User processUser(Long id) {
     *       User user = userService.findById(id);  // 阻塞等待数据库查询
     *       user.setStatus("processed");           // 阻塞执行业务逻辑
     *       userService.save(user);                // 阻塞等待保存
     *       return user;                           // 线程一直被占用
     *   }
     * 
     * 在WebFlux中：
     *   返回Mono，通过链式调用组合异步操作
     */
    @GetMapping("/process/{id}")
    public Mono<String> processUser(@PathVariable Long id) {
        return Mono.just(id)
                // 模拟数据库查询（异步操作）
                .flatMap(userId -> {
                    System.out.println("查询用户 " + userId + "，线程：" + Thread.currentThread().getName());
                    return Mono.just("User-" + userId)
                            .delayElement(Duration.ofMillis(100)); // 模拟IO延迟
                })
                // 业务处理（异步操作）
                .map(user -> {
                    System.out.println("处理用户 " + user + "，线程：" + Thread.currentThread().getName());
                    return user + "-processed";
                })
                // 模拟保存（异步操作）
                .flatMap(user -> {
                    System.out.println("保存用户 " + user + "，线程：" + Thread.currentThread().getName());
                    return Mono.just(user)
                            .delayElement(Duration.ofMillis(100));
                });
        
        // 注意：这个方法会立即返回，不会阻塞线程！
        // WebFlux框架会自动订阅(subscribe)这个Mono，当数据准备好时自动写回响应
    }

    /**
     * 示例3：为什么要这样做？
     * 
     * 核心原因：线程利用率
     * 
     * Servlet模型（同步阻塞）：
     * ┌─────────┐
     * │ Thread1 │ ━━━━[等待DB]━━━━[处理]━━━━[等待保存]━━━━ 返回
     * │ Thread2 │ ━━━━[等待DB]━━━━[处理]━━━━[等待保存]━━━━ 返回
     * │ Thread3 │ ━━━━[等待DB]━━━━[处理]━━━━[等待保存]━━━━ 返回
     * └─────────┘
     * 每个请求占用一个线程，线程在等待IO时被浪费
     * 
     * WebFlux模型（异步非阻塞）：
     * ┌─────────┐
     * │ Thread1 │ ─[请求1]─[请求2]─[请求3]─[请求4]─[请求5]─
     * │ Thread2 │ ─[请求6]─[请求7]─[请求8]─[请求9]─[请求10]
     * └─────────┘
     * 少量线程处理大量请求，线程在等待IO时可以处理其他请求
     */
    @GetMapping("/compare/blocking")
    public Mono<String> compareBlocking() {
        long start = System.currentTimeMillis();
        
        // 如果这是Servlet，下面的代码会阻塞线程
        // Thread.sleep(1000);  // 阻塞1秒
        
        // 在WebFlux中，使用非阻塞的延迟
        return Mono.delay(Duration.ofSeconds(1))
                .map(tick -> {
                    long end = System.currentTimeMillis();
                    return "处理完成，耗时：" + (end - start) + "ms，线程：" + Thread.currentThread().getName();
                });
    }

    // ============================================================
    // 问题2：在WebFlux中如何请求其他服务并返回数据？
    // ============================================================

    /**
     * 示例4：调用其他服务
     * 
     * 在Servlet中：
     *   @GetMapping("/call-service")
     *   public String callOtherService() {
     *       RestTemplate restTemplate = new RestTemplate();
     *       String result = restTemplate.getForObject("http://other-service/api", String.class);
     *       // 线程阻塞，等待HTTP响应
     *       String processed = processResult(result);  // 处理业务
     *       return processed;
     *   }
     * 
     * 在WebFlux中：
     *   使用WebClient（非阻塞的HTTP客户端）
     */
    @GetMapping("/call-service")
    public Mono<String> callOtherService() {
        System.out.println("开始调用服务，线程：" + Thread.currentThread().getName());
        
        return webClient
                .method(HttpMethod.GET)
                .uri("http://localhost:8080/example/simple")  // 调用自己的接口做演示
                .retrieve()
                .bodyToMono(String.class)  // 返回Mono<String>，不阻塞
                .map(result -> {
                    System.out.println("收到响应：" + result + "，线程：" + Thread.currentThread().getName());
                    return "处理后的结果：" + result;
                });
        
        // 这个方法立即返回，不会阻塞等待HTTP响应
    }

    /**
     * 示例5：调用多个服务并组合结果
     * 
     * 在Servlet中：
     *   @GetMapping("/call-multiple")
     *   public Result callMultipleServices() {
     *       RestTemplate restTemplate = new RestTemplate();
     *       String service1 = restTemplate.getForObject("http://service1/api", String.class);  // 阻塞1秒
     *       String service2 = restTemplate.getForObject("http://service2/api", String.class);  // 阻塞1秒
     *       String service3 = restTemplate.getForObject("http://service3/api", String.class);  // 阻塞1秒
     *       // 总耗时：3秒（串行执行）
     *       return combine(service1, service2, service3);
     *   }
     * 
     * 在WebFlux中：
     *   可以并行调用，大大提高效率
     */
    @GetMapping("/call-multiple")
    public Mono<String> callMultipleServices() {
        long start = System.currentTimeMillis();
        
        // 创建3个异步调用（并行执行）
        Mono<String> service1 = webClient.get()
                .uri("http://localhost:8080/example/simple")
                .retrieve()
                .bodyToMono(String.class)
                .delayElement(Duration.ofSeconds(1));  // 模拟1秒延迟

        Mono<String> service2 = webClient.get()
                .uri("http://localhost:8080/example/simple")
                .retrieve()
                .bodyToMono(String.class)
                .delayElement(Duration.ofSeconds(1));  // 模拟1秒延迟

        Mono<String> service3 = webClient.get()
                .uri("http://localhost:8080/example/simple")
                .retrieve()
                .bodyToMono(String.class)
                .delayElement(Duration.ofSeconds(1));  // 模拟1秒延迟

        // 并行执行，等待所有完成后组合结果
        return Mono.zip(service1, service2, service3)
                .map(tuple -> {
                    long end = System.currentTimeMillis();
                    String result1 = tuple.getT1();
                    String result2 = tuple.getT2();
                    String result3 = tuple.getT3();
                    return String.format("组合结果：[%s, %s, %s]，总耗时：%dms（并行执行）", 
                            result1, result2, result3, (end - start));
                });
        
        // 在Servlet中需要3秒（串行），在WebFlux中只需要1秒（并行）！
    }

    /**
     * 示例6：调用服务 -> 处理业务 -> 再调用服务 -> 返回
     * 
     * 这是最常见的场景
     */
    @GetMapping("/complex-flow/{userId}")
    public Mono<String> complexBusinessFlow(@PathVariable String userId) {
        System.out.println("开始处理请求，线程：" + Thread.currentThread().getName());
        
        return webClient.get()
                // 步骤1：调用用户服务获取用户信息
                .uri("http://localhost:8080/example/simple")
                .retrieve()
                .bodyToMono(String.class)
                .doOnNext(user -> System.out.println("获取到用户：" + user + "，线程：" + Thread.currentThread().getName()))
                
                // 步骤2：处理业务逻辑（可能需要计算）
                .flatMap(user -> {
                    System.out.println("处理业务逻辑，线程：" + Thread.currentThread().getName());
                    // 模拟CPU密集型操作，切换到计算线程池
                    return Mono.fromCallable(() -> {
                        // 这里可以进行复杂计算
                        return "Processed-" + user + "-" + userId;
                    }).subscribeOn(Schedulers.parallel());
                })
                
                // 步骤3：调用订单服务创建订单
                .flatMap(processedData -> {
                    System.out.println("创建订单，线程：" + Thread.currentThread().getName());
                    return webClient.post()
                            .uri("http://localhost:8080/example/simple")
                            .bodyValue(processedData)
                            .retrieve()
                            .bodyToMono(String.class);
                })
                
                // 步骤4：返回最终结果
                .map(orderResult -> {
                    System.out.println("返回结果，线程：" + Thread.currentThread().getName());
                    return "最终结果：" + orderResult;
                });
        
        // 整个流程中，线程从不阻塞等待！
    }

    /**
     * 示例7：错误处理
     * 
     * 在WebFlux中，错误处理也是响应式的
     */
    @GetMapping("/with-error-handling")
    public Mono<String> withErrorHandling() {
        return webClient.get()
                .uri("http://localhost:8080/example/simple")
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(5))  // 设置超时
                .onErrorResume(throwable -> {
                    // 如果出错，返回默认值
                    System.err.println("调用失败：" + throwable.getMessage());
                    return Mono.just("默认值");
                })
                .doOnError(throwable -> {
                    // 记录错误日志
                    System.err.println("发生错误：" + throwable.getMessage());
                });
    }

    // ============================================================
    // 总结
    // ============================================================
    
    /**
     * WebFlux vs Servlet 核心区别总结：
     * 
     * 1. 返回类型：
     *    - Servlet: 返回具体对象（如String, User等）
     *    - WebFlux: 返回Mono<T>或Flux<T>（响应式类型）
     * 
     * 2. 执行模型：
     *    - Servlet: 同步阻塞，一个请求占用一个线程直到完成
     *    - WebFlux: 异步非阻塞，少量线程处理大量请求
     * 
     * 3. 调用外部服务：
     *    - Servlet: 使用RestTemplate（阻塞）
     *    - WebFlux: 使用WebClient（非阻塞）
     * 
     * 4. 为什么要这样做：
     *    - 提高吞吐量：相同硬件条件下可以处理更多请求
     *    - 降低延迟：IO等待时可以处理其他请求
     *    - 节省资源：不需要为每个请求创建线程
     *    - 支持背压：可以控制数据流速度，防止内存溢出
     * 
     * 5. 何时使用WebFlux：
     *    - 高并发场景（如实时消息、流式数据）
     *    - 需要调用多个外部服务
     *    - IO密集型应用
     *    - 需要服务端推送（SSE、WebSocket）
     * 
     * 6. 何时不适合WebFlux：
     *    - CPU密集型应用（大量计算）
     *    - 使用阻塞的第三方库（如JDBC）
     *    - 团队对响应式编程不熟悉
     */
}

