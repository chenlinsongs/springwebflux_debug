package org.example.springwebflux.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示多次订阅的问题
 * 
 * 展示手动 subscribe + 返回 Mono 会导致执行2次
 */
@RestController
@RequestMapping("/multiple-subscribe")
public class MultipleSubscribeController {

    /**
     * 演示1：❌ 错误做法 - 手动 subscribe + 返回 Mono
     * 
     * 访问：http://localhost:8080/multiple-subscribe/wrong-way
     * 
     * 观察控制台：会看到执行了2次
     */
    @GetMapping("/wrong-way")
    public Mono<String> wrongWay() {
        System.out.println("\n=== ❌ 错误做法：手动 subscribe + 返回 Mono ===");
        
        AtomicInteger executionCount = new AtomicInteger(0);
        
        Mono<String> mono = Mono.fromCallable(() -> {
            int count = executionCount.incrementAndGet();
            System.out.println(">>> 执行次数：" + count + "，线程：" + Thread.currentThread().getName());
            return "结果-" + count;
        });
        
        // ❌ 错误：手动 subscribe
        System.out.println("1. 手动 subscribe");
        mono.subscribe(result -> {
            System.out.println("   手动 subscribe 收到：" + result);
        });
        
        System.out.println("2. 返回 Mono 给框架");
        System.out.println("3. 框架会再次 subscribe\n");
        
        // 返回给框架（框架会再次 subscribe）
        return mono;
        
        // 结果：执行了2次！
        // 客户端收到的是第2次执行的结果
    }

    /**
     * 演示2：✅ 正确做法 - 使用 doOnNext
     * 
     * 访问：http://localhost:8080/multiple-subscribe/correct-way
     * 
     * 观察控制台：只执行1次
     */
    @GetMapping("/correct-way")
    public Mono<String> correctWay() {
        System.out.println("\n=== ✅ 正确做法：使用 doOnNext ===");
        
        AtomicInteger executionCount = new AtomicInteger(0);
        
        return Mono.fromCallable(() -> {
            int count = executionCount.incrementAndGet();
            System.out.println(">>> 执行次数：" + count + "，线程：" + Thread.currentThread().getName());
            return "结果-" + count;
        })
        .doOnNext(result -> {
            System.out.println("   doOnNext 收到：" + result);
        });
        
        // 结果：只执行1次
        // doOnNext 在框架 subscribe 时触发
    }

    /**
     * 演示3：数据库查询重复执行的问题
     * 
     * 访问：http://localhost:8080/multiple-subscribe/db-query-duplicate
     */
    @GetMapping("/db-query-duplicate")
    public Mono<User> dbQueryDuplicate() {
        System.out.println("\n=== 数据库查询重复执行问题 ===");
        
        Mono<User> mono = Mono.fromCallable(() -> {
            System.out.println(">>> 查询数据库...");
            // 模拟数据库查询
            Thread.sleep(100);
            User user = new User(1L, "张三");
            System.out.println(">>> 查询完成：" + user);
            return user;
        });
        
        // ❌ 错误：手动 subscribe
        System.out.println("1. 手动 subscribe（用于打印日志）");
        mono.subscribe(user -> {
            System.out.println("   手动收到用户：" + user.getName());
        });
        
        System.out.println("2. 返回 Mono\n");
        
        // 返回给框架
        return mono;
        
        // 问题：数据库被查询了2次！
    }

    /**
     * 演示4：正确的日志记录方式
     * 
     * 访问：http://localhost:8080/multiple-subscribe/db-query-correct
     */
    @GetMapping("/db-query-correct")
    public Mono<User> dbQueryCorrect() {
        System.out.println("\n=== 正确的日志记录方式 ===");
        
        return Mono.fromCallable(() -> {
            System.out.println(">>> 查询数据库...");
            // 模拟数据库查询
            Thread.sleep(100);
            User user = new User(1L, "张三");
            System.out.println(">>> 查询完成：" + user);
            return user;
        })
        .doOnNext(user -> {
            System.out.println("   doOnNext：用户名 = " + user.getName());
        });
        
        // 结果：数据库只查询1次
    }

    /**
     * 演示5：副作用重复执行的问题
     * 
     * 访问：http://localhost:8080/multiple-subscribe/side-effect-duplicate
     */
    @GetMapping("/side-effect-duplicate")
    public Mono<Order> sideEffectDuplicate() {
        System.out.println("\n=== 副作用重复执行问题 ===");
        
        AtomicInteger orderIdGenerator = new AtomicInteger(1000);
        
        Mono<Order> mono = Mono.fromCallable(() -> {
            int orderId = orderIdGenerator.incrementAndGet();
            System.out.println(">>> 创建订单，ID = " + orderId);
            return new Order(orderId, "商品A");
        });
        
        // ❌ 错误：手动 subscribe 发送通知
        System.out.println("1. 手动 subscribe（发送通知）");
        mono.subscribe(order -> {
            System.out.println("   发送通知：订单 " + order.getId() + " 创建成功");
        });
        
        System.out.println("2. 返回 Mono\n");
        
        return mono;
        
        // 问题：
        // - 订单创建了2次
        // - 通知发送了1次（但应该只发送给第2次创建的订单）
    }

    /**
     * 演示6：正确的副作用处理
     * 
     * 访问：http://localhost:8080/multiple-subscribe/side-effect-correct
     */
    @GetMapping("/side-effect-correct")
    public Mono<Order> sideEffectCorrect() {
        System.out.println("\n=== 正确的副作用处理 ===");
        
        AtomicInteger orderIdGenerator = new AtomicInteger(2000);
        
        return Mono.fromCallable(() -> {
            int orderId = orderIdGenerator.incrementAndGet();
            System.out.println(">>> 创建订单，ID = " + orderId);
            return new Order(orderId, "商品B");
        })
        .doOnSuccess(order -> {
            System.out.println("   doOnSuccess：发送通知，订单 " + order.getId() + " 创建成功");
        });
        
        // 结果：订单只创建1次，通知只发送1次
    }

    /**
     * 演示7：使用 cache() 的情况
     * 
     * 访问：http://localhost:8080/multiple-subscribe/with-cache
     */
    @GetMapping("/with-cache")
    public Mono<String> withCache() {
        System.out.println("\n=== 使用 cache() ===");
        
        AtomicInteger executionCount = new AtomicInteger(0);
        
        // ✅ 使用 cache() 缓存结果
        Mono<String> mono = Mono.fromCallable(() -> {
            int count = executionCount.incrementAndGet();
            System.out.println(">>> 执行次数：" + count);
            return "结果-" + count;
        }).cache();  // 缓存结果
        
        // 第1次 subscribe
        System.out.println("1. 第1次 subscribe");
        mono.subscribe(result -> {
            System.out.println("   第1次收到：" + result);
        });
        
        // 第2次 subscribe
        System.out.println("2. 第2次 subscribe");
        mono.subscribe(result -> {
            System.out.println("   第2次收到：" + result);
        });
        
        System.out.println("3. 返回 Mono（框架会第3次 subscribe）\n");
        
        return mono;
        
        // 结果：只执行1次，3个 subscribe 都收到相同的缓存结果
    }

    /**
     * 演示8：对比3种方式
     * 
     * 访问：http://localhost:8080/multiple-subscribe/comparison
     */
    @GetMapping("/comparison")
    public Mono<String> comparison() {
        System.out.println("\n=== 对比3种方式 ===");
        
        // 方式1：手动 subscribe + 返回 Mono（❌ 错误）
        System.out.println("\n--- 方式1：手动 subscribe + 返回 Mono ---");
        AtomicInteger count1 = new AtomicInteger(0);
        Mono<String> mono1 = Mono.fromCallable(() -> "执行次数-" + count1.incrementAndGet());
        mono1.subscribe(r -> System.out.println("手动收到：" + r));
        String result1 = mono1.block();  // 模拟框架 subscribe
        System.out.println("框架收到：" + result1);
        System.out.println("总执行次数：" + count1.get());
        
        // 方式2：使用 doOnNext（✅ 正确）
        System.out.println("\n--- 方式2：使用 doOnNext ---");
        AtomicInteger count2 = new AtomicInteger(0);
        Mono<String> mono2 = Mono.fromCallable(() -> "执行次数-" + count2.incrementAndGet())
                .doOnNext(r -> System.out.println("doOnNext收到：" + r));
        String result2 = mono2.block();  // 模拟框架 subscribe
        System.out.println("框架收到：" + result2);
        System.out.println("总执行次数：" + count2.get());
        
        // 方式3：使用 cache()（⚠️ 特殊场景）
        System.out.println("\n--- 方式3：使用 cache() ---");
        AtomicInteger count3 = new AtomicInteger(0);
        Mono<String> mono3 = Mono.fromCallable(() -> "执行次数-" + count3.incrementAndGet()).cache();
        mono3.subscribe(r -> System.out.println("手动收到：" + r));
        String result3 = mono3.block();  // 模拟框架 subscribe
        System.out.println("框架收到：" + result3);
        System.out.println("总执行次数：" + count3.get());
        
        return Mono.just(
                "对比结果：\n" +
                "方式1（手动subscribe）：执行" + count1.get() + "次 ❌\n" +
                "方式2（doOnNext）：执行" + count2.get() + "次 ✅\n" +
                "方式3（cache）：执行" + count3.get() + "次 ✅\n\n" +
                "查看控制台了解详情"
        );
    }

    /**
     * 演示9：实际业务场景 - 创建订单
     * 
     * 访问：http://localhost:8080/multiple-subscribe/create-order-wrong
     */
    @GetMapping("/create-order-wrong")
    public Mono<OrderResult> createOrderWrong() {
        System.out.println("\n=== ❌ 错误：创建订单 ===");
        
        AtomicInteger orderCounter = new AtomicInteger(3000);
        
        Mono<OrderResult> mono = Mono.fromCallable(() -> {
            int orderId = orderCounter.incrementAndGet();
            System.out.println(">>> 数据库：创建订单 " + orderId);
            System.out.println(">>> 数据库：扣减库存");
            System.out.println(">>> 数据库：更新用户积分");
            return new OrderResult(orderId, "订单创建成功");
        });
        
        // ❌ 错误：手动 subscribe 记录日志
        mono.subscribe(result -> {
            System.out.println("   手动收到：" + result.getMessage());
        });
        
        return mono;
        
        // 问题：
        // - 订单创建了2次
        // - 库存扣减了2次
        // - 积分更新了2次
        // - 严重的业务问题！
    }

    /**
     * 演示10：实际业务场景 - 创建订单（正确）
     * 
     * 访问：http://localhost:8080/multiple-subscribe/create-order-correct
     */
    @GetMapping("/create-order-correct")
    public Mono<OrderResult> createOrderCorrect() {
        System.out.println("\n=== ✅ 正确：创建订单 ===");
        
        AtomicInteger orderCounter = new AtomicInteger(4000);
        
        return Mono.fromCallable(() -> {
            int orderId = orderCounter.incrementAndGet();
            System.out.println(">>> 数据库：创建订单 " + orderId);
            System.out.println(">>> 数据库：扣减库存");
            System.out.println(">>> 数据库：更新用户积分");
            return new OrderResult(orderId, "订单创建成功");
        })
        .doOnSuccess(result -> {
            System.out.println("   doOnSuccess：订单创建成功，ID = " + result.getOrderId());
        });
        
        // 结果：所有操作只执行1次
    }

    /**
     * 总结接口
     */
    @GetMapping("/summary")
    public Mono<String> summary() {
        return Mono.just(
                "=== 多次订阅问题总结 ===\n\n" +
                
                "1. 问题：手动 subscribe + 返回 Mono\n" +
                "   结果：执行2次（你的 + 框架的）\n\n" +
                
                "2. 危害：\n" +
                "   - 数据库查询重复\n" +
                "   - 订单重复创建\n" +
                "   - 通知重复发送\n" +
                "   - 计数器重复增加\n\n" +
                
                "3. 解决方案：\n" +
                "   ✅ 使用 doOnNext/doOnSuccess\n" +
                "   ✅ 特殊场景使用 cache()\n" +
                "   ❌ 不要手动 subscribe + 返回 Mono\n\n" +
                
                "4. 测试接口：\n" +
                "   - /multiple-subscribe/wrong-way - 错误示例\n" +
                "   - /multiple-subscribe/correct-way - 正确示例\n" +
                "   - /multiple-subscribe/db-query-duplicate - 数据库问题\n" +
                "   - /multiple-subscribe/side-effect-duplicate - 副作用问题\n" +
                "   - /multiple-subscribe/create-order-wrong - 业务问题\n" +
                "   - /multiple-subscribe/comparison - 对比\n\n" +
                
                "⚠️ 记住：手动 subscribe + 返回 Mono = 执行2次！"
        );
    }

    // 辅助类
    static class User {
        private Long id;
        private String name;

        public User(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() { return id; }
        public String getName() { return name; }

        @Override
        public String toString() {
            return "User{id=" + id + ", name='" + name + "'}";
        }
    }

    static class Order {
        private Integer id;
        private String product;

        public Order(Integer id, String product) {
            this.id = id;
            this.product = product;
        }

        public Integer getId() { return id; }
        public String getProduct() { return product; }
    }

    static class OrderResult {
        private Integer orderId;
        private String message;

        public OrderResult(Integer orderId, String message) {
            this.orderId = orderId;
            this.message = message;
        }

        public Integer getOrderId() { return orderId; }
        public String getMessage() { return message; }
    }
}

