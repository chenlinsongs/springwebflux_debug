package org.example.springwebflux.controller.very;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2025/11/22 12:16
 * @Description:
 */
@RestController
@RequestMapping("/verify")
public class VerifyController {

    /**
     * 方式1: 使用 Mono.create() - 验证 Mono返回后，上层会注册订阅者到Mono，只有Mono写入数据后才会有返回值，这个数据写入由MonoSink完成
     * 适用场景：需要手动控制数据发射时机，比如异步回调
     * */
    @GetMapping("/create")
    public Mono<Integer> monoCreate() {
        Mono<Integer> mono = Mono.create(new Consumer<MonoSink<Integer>>() {
            @Override
            public void accept(MonoSink<Integer> integerMonoSink) {
                integerMonoSink.success(1);
            }
        });
        return mono;
    }

    /**
     * 方式2: 使用 Mono.just() - 最简单直接的方式，直接返回一个已知值
     * 适用场景：已经有确定的值需要返回
     * 注意：值在创建时就已确定，不是延迟计算的
     * */
    @GetMapping("/just")
    public Mono<Integer> monoJust() {
        return Mono.just(2);
    }

    /**
     * 方式3: 使用 Mono.fromSupplier() - 延迟获取值，订阅时才执行Supplier
     * 适用场景：值需要在订阅时才计算，或者计算比较耗时
     * */
    @GetMapping("/fromSupplier")
    public Mono<Integer> monoFromSupplier() {
        Mono mono = Mono.fromSupplier(new Supplier<Integer>() {
            @Override
            public Integer get() {
                int value = (int) (Math.random() * 100);
                System.out.println("Supplier执行了，返回值+"+value);
                return 3;
            }
        });
        mono.subscribe(new Consumer() {
            @Override
            public void accept(Object o) {
                System.out.println(o.toString());
            }
        });
        mono.subscribe(new Consumer() {
            @Override
            public void accept(Object o) {
                System.out.println(o.toString());
            }
        });
        return mono;
    }

    /**
     * 方式4: 使用 Mono.fromCallable() - 类似fromSupplier，但可以抛出异常
     * 适用场景：需要执行可能抛异常的计算
     * */
    @GetMapping("/fromCallable")
    public Mono<Integer> monoFromCallable() {
        return Mono.fromCallable(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                System.out.println("Callable执行了，返回值4");
                // 可以抛出检查异常
                return 4;
            }
        });
    }

    /**
     * 方式5: 使用 Mono.defer() - 延迟创建整个Mono，每次订阅都会重新创建
     * 适用场景：每次订阅都需要不同的值或不同的逻辑
     * */
    @GetMapping("/defer")
    public Mono<Integer> monoDefer() {
        Mono mono = Mono.defer(() -> {
            System.out.println("Defer执行了，每次订阅都会执行这里");
            int value = (int) (Math.random() * 100); // 每次订阅产生不同的随机数
            return Mono.just(value);
        });
        mono.subscribe(new Consumer() {
            @Override
            public void accept(Object o) {
                System.out.println("one:"+o.toString());
            }
        });
        mono.subscribe(new Consumer() {
            @Override
            public void accept(Object o) {
                System.out.println("two:"+o.toString());
            }
        });
        return mono;
    }

    /**
     * 方式6: 使用 Mono.justOrEmpty() - 返回可能为空的值
     * 适用场景：值可能为null，需要安全处理
     * */
    @GetMapping("/justOrEmpty/{value}")
    public Mono<Integer> monoJustOrEmpty(@PathVariable(required = false) Integer value) {
        return Mono.justOrEmpty(value); // value为null时返回空Mono
    }

    /**
     * 方式7: 使用 Mono.empty() - 返回空的Mono，不发射任何数据
     * 适用场景：不需要返回数据，只需要完成信号
     * */
    @GetMapping("/empty")
    public Mono<Integer> monoEmpty() {
        return Mono.empty();
    }

    /**
     * 方式8: 使用 Mono.fromFuture() - 从CompletableFuture获取值
     * 适用场景：已有CompletableFuture，需要转换为Mono
     * */
    @GetMapping("/fromFuture")
    public Mono<Integer> monoFromFuture() {
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
            System.out.println("CompletableFuture执行了");
            try {
                Thread.sleep(1000); // 模拟异步操作
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("线程被中断", e);
            }
            return 8;
        });
        return Mono.fromFuture(future);
    }

    /**
     * 方式9: 使用 Mono.fromRunnable() - 执行Runnable后返回空Mono
     * 适用场景：需要执行某个操作但不需要返回值
     * */
    @GetMapping("/fromRunnable")
    public Mono<Void> monoFromRunnable() {
        return Mono.fromRunnable(() -> {
            System.out.println("执行了Runnable，但不返回值");
        });
    }

    /**
     * 方式10: 使用 Mono.delay() - 延迟一段时间后返回
     * 适用场景：需要延迟返回
     * */
    @GetMapping("/delay")
    public Mono<Long> monoDelay() {
        return Mono.delay(Duration.ofSeconds(2)); // 2秒后返回0
    }

    /**
     * 方式11: 使用 Flux.next() - 从Flux中获取第一个元素作为Mono
     * 适用场景：有Flux需要只取第一个元素
     * */
    @GetMapping("/fromFlux")
    public Mono<Integer> monoFromFlux() {
        Flux<Integer> flux = Flux.just(11, 12, 13, 14);
        return flux.next(); // 只取第一个元素11
    }

    /**
     * 方式12: 链式操作创建 - 通过map、flatMap等操作符转换
     * 适用场景：基于已有Mono进行转换
     * */
    @GetMapping("/chain")
    public Mono<String> monoChain() {
        return Mono.just(12)
                .map(i -> i * 2)           // 24
                .map(i -> "Result: " + i); // "Result: 24"
    }

    // ==================== create vs fromSupplier 核心区别演示 ====================

    /**
     * 区别演示1: fromSupplier - 同步执行，必须立即返回结果
     * 特点：Supplier的get()方法被调用时，必须立即返回一个值
     * 限制：无法处理真正的异步场景（如异步回调）
     * */
    @GetMapping("/difference/supplier")
    public Mono<String> differenceSupplier() {
        System.out.println("【Supplier】创建Mono");
        
        return Mono.fromSupplier(() -> {
            System.out.println("【Supplier】订阅时执行 - 线程: " + Thread.currentThread().getName());
            
            // Supplier必须同步返回结果，不能延迟
            // 即使你想做异步操作，也必须在这里等待结果
            try {
                Thread.sleep(1000); // 只能用阻塞的方式等待
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            System.out.println("【Supplier】返回结果");
            return "Supplier方式: 同步返回";
        });
    }

    /**
     * 区别演示2: create - 异步执行，可以在任意时间点发射数据
     * 特点：通过MonoSink，可以在未来某个时间点才发射数据
     * 优势：真正支持异步场景，不阻塞线程
     * */
    @GetMapping("/difference/create")
    public Mono<String> differenceCreate() {
        System.out.println("【Create】创建Mono");
        
        return Mono.create(sink -> {
            System.out.println("【Create】订阅时执行 - 线程: " + Thread.currentThread().getName());
            
            // create可以真正异步：在另一个线程中延迟发射数据
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    System.out.println("【Create】异步线程发射数据 - 线程: " + Thread.currentThread().getName());
                    sink.success("Create方式: 异步返回");
                } catch (InterruptedException e) {
                    sink.error(e);
                }
            }).start();
            
            // 注意：这里立即返回，不会阻塞！数据会在1秒后由另一个线程发射
            System.out.println("【Create】Consumer立即返回，不阻塞");
        });
    }

    /**
     * 区别演示3: fromSupplier无法处理异步回调场景
     * 问题：如果你有一个异步回调，Supplier无法处理
     * */
    @GetMapping("/difference/supplier-async-wrong")
    public Mono<String> supplierAsyncWrong() {
        return Mono.fromSupplier(() -> {
            System.out.println("【Supplier异步尝试】开始");
            
            // ❌ 错误示范：启动异步任务后立即返回
            // 问题：Supplier必须立即返回值，异步任务的结果无法传递出去
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    System.out.println("【Supplier异步尝试】异步任务完成，但这个结果无法返回！");
                    // 这里的结果永远无法通过Supplier返回
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
            
            // ❌ 只能返回这个立即值，异步任务的结果丢失了
            return "立即返回的值（异步结果丢失了）";
        });
    }

    /**
     * 区别演示4: create可以完美处理异步回调场景
     * 优势：MonoSink可以在任意时间点调用success()发射数据
     * */
    @GetMapping("/difference/create-async-right")
    public Mono<String> createAsyncRight() {
        return Mono.create(sink -> {
            System.out.println("【Create异步】开始");
            
            // ✅ 正确做法：异步任务完成后通过sink发射数据
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    String result = "异步任务的真实结果";
                    System.out.println("【Create异步】异步任务完成，通过sink发射: " + result);
                    sink.success(result); // ✅ 异步结果可以正确返回
                } catch (InterruptedException e) {
                    sink.error(e);
                }
            }).start();
            
            System.out.println("【Create异步】Consumer立即返回，等待异步任务");
        });
    }

    /**
     * 区别演示5: 模拟真实的异步回调场景（如异步HTTP客户端）
     * 场景：模拟一个异步API调用，回调在未来某个时间点触发
     * */
    @GetMapping("/difference/callback-scenario")
    public Mono<String> callbackScenario() {
        return Mono.create(sink -> {
            System.out.println("【回调场景】发起异步调用");
            
            // 模拟异步API调用（如OkHttp异步请求、异步消息队列等）
            simulateAsyncApiCall(new AsyncCallback() {
                @Override
                public void onSuccess(String result) {
                    System.out.println("【回调场景】回调成功: " + result);
                    sink.success(result); // ✅ 通过sink发射回调结果
                }
                
                @Override
                public void onError(Exception e) {
                    System.out.println("【回调场景】回调失败");
                    sink.error(e); // ✅ 通过sink发射错误
                }
            });
            
            System.out.println("【回调场景】注册回调后立即返回");
        });
    }
    
    // 模拟异步API
    private void simulateAsyncApiCall(AsyncCallback callback) {
        new Thread(() -> {
            try {
                Thread.sleep(1500);
                callback.onSuccess("异步API返回的数据");
            } catch (InterruptedException e) {
                callback.onError(e);
            }
        }).start();
    }
    
    // 回调接口
    interface AsyncCallback {
        void onSuccess(String result);
        void onError(Exception e);
    }

    // ==================== fromSupplier vs defer 核心区别演示 ====================

    /**
     * 区别关键: fromSupplier vs defer
     * 
     * fromSupplier: 延迟计算"值"
     * defer: 延迟创建整个"Mono流"
     * */
    @GetMapping("/supplier-vs-defer/simple")
    public Mono<String> supplierVsDeferSimple() {
        System.out.println("\n========== 简单场景对比 ==========");
        
        // fromSupplier: 延迟计算值
        Mono<String> supplier = Mono.fromSupplier(() -> {
            System.out.println("【fromSupplier】计算值");
            return "随机数: " + (int)(Math.random() * 100);
        });
        
        // defer: 延迟创建Mono
        Mono<String> defer = Mono.defer(() -> {
            System.out.println("【defer】创建Mono");
            return Mono.just("随机数: " + (int)(Math.random() * 100));
        });
        
        System.out.println("\n第一次订阅 fromSupplier:");
        supplier.subscribe(v -> System.out.println("  结果: " + v));
        
        System.out.println("\n第二次订阅 fromSupplier:");
        supplier.subscribe(v -> System.out.println("  结果: " + v));
        
        System.out.println("\n第一次订阅 defer:");
        defer.subscribe(v -> System.out.println("  结果: " + v));
        
        System.out.println("\n第二次订阅 defer:");
        defer.subscribe(v -> System.out.println("  结果: " + v));
        
        return Mono.just("在这个简单场景下，它们效果一样！但看下一个示例...");
    }

    /**
     * 核心区别1: defer可以动态决定创建什么类型的Mono
     * fromSupplier只能返回值，而defer可以返回Mono.just/Mono.error/Mono.empty等
     * */
    @GetMapping("/supplier-vs-defer/dynamic/{flag}")
    public Mono<String> supplierVsDeferDynamic(@PathVariable String flag) {
        System.out.println("\n========== 动态创建Mono ==========");
        
        // ❌ fromSupplier: 只能返回值，无法根据条件返回不同类型的Mono
        // 如果想返回error或empty，只能抛异常或返回null（不优雅）
        System.out.println("【fromSupplier】只能这样处理:");
        System.out.println("  - 返回error: 只能抛异常");
        System.out.println("  - 返回empty: 只能返回null（不优雅）");
        
        // ✅ defer: 可以优雅地根据条件返回不同类型的Mono
        Mono<String> defer = Mono.defer(() -> {
            System.out.println("【defer】根据flag=" + flag + "创建不同的Mono");
            if ("error".equals(flag)) {
                return Mono.error(new RuntimeException("优雅地返回error Mono"));
            }
            if ("empty".equals(flag)) {
                return Mono.empty(); // 优雅地返回空Mono
            }
            return Mono.just("success");
        });
        
        return defer; // defer更灵活！
    }

    /**
     * 核心区别2: defer可以动态构建不同的操作符链
     * fromSupplier只能返回值，defer可以返回带有不同操作符的Mono
     * */
    @GetMapping("/supplier-vs-defer/chain/{scenario}")
    public Mono<String> supplierVsDeferChain(@PathVariable String scenario) {
        System.out.println("\n========== 动态操作符链 ==========");
        
        // ❌ fromSupplier: 操作符链是固定的，只是值不同
        System.out.println("【fromSupplier】操作符链在创建时固定:");
        System.out.println("  Mono.fromSupplier(...).map(...).filter(...)");
        System.out.println("  只是Supplier返回的值不同，map和filter是固定的");
        
        // ✅ defer: 可以根据场景创建完全不同的操作符链
        Mono<String> defer = Mono.defer(() -> {
            System.out.println("【defer】根据scenario=" + scenario + "创建不同的处理链");
            
            if ("fast".equals(scenario)) {
                // 快速场景：直接返回
                return Mono.just("快速响应");
            } else if ("slow".equals(scenario)) {
                // 慢速场景：添加延迟
                return Mono.just("慢速响应")
                        .delayElement(Duration.ofSeconds(2));
            } else if ("transform".equals(scenario)) {
                // 转换场景：复杂的操作符链
                return Mono.just("原始值")
                        .map(String::toUpperCase)
                        .map(s -> s + " - 已转换")
                        .flatMap(s -> Mono.just(s + " - 已增强"));
            } else {
                return Mono.just("默认场景");
            }
        });
        
        return defer; // defer可以动态选择处理逻辑！
    }

    /**
     * 核心区别3: defer在订阅时捕获上下文
     * 可以根据订阅时的状态（而不是创建时的状态）做决策
     * */
    private int counter = 0;
    
    @GetMapping("/supplier-vs-defer/context")
    public Mono<String> supplierVsDeferContext() {
        System.out.println("\n========== 上下文捕获 ==========");
        System.out.println("创建时 counter = " + counter);
        
        // fromSupplier: 也是订阅时执行，能获取订阅时的状态
        Mono<String> supplier = Mono.fromSupplier(() -> {
            int current = counter++;
            System.out.println("【fromSupplier】订阅时 counter = " + current);
            return "fromSupplier看到的counter: " + current;
        });
        
        // defer: 订阅时执行，能获取订阅时的状态，但可以基于此创建不同的Mono
        Mono<String> defer = Mono.defer(() -> {
            int current = counter++;
            System.out.println("【defer】订阅时 counter = " + current);
            
            // defer的优势：可以根据订阅时的状态创建不同的Mono
            if (current < 5) {
                return Mono.just("defer: counter较小(" + current + ")，返回普通值");
            } else if (current < 10) {
                return Mono.just("defer: counter中等(" + current + ")，添加处理")
                        .map(String::toUpperCase);
            } else {
                return Mono.error(new RuntimeException("defer: counter太大了(" + current + ")"));
            }
        });
        
        System.out.println("\n第1次订阅 supplier:");
        supplier.subscribe(v -> System.out.println("  " + v));
        
        System.out.println("\n第2次订阅 supplier:");
        supplier.subscribe(v -> System.out.println("  " + v));
        
        System.out.println("\n第1次订阅 defer:");
        defer.subscribe(
            v -> System.out.println("  " + v),
            e -> System.out.println("  错误: " + e.getMessage())
        );
        
        System.out.println("\n第2次订阅 defer:");
        defer.subscribe(
            v -> System.out.println("  " + v),
            e -> System.out.println("  错误: " + e.getMessage())
        );
        
        return Mono.just("defer能根据状态创建不同的Mono，fromSupplier只能返回不同的值");
    }

    /**
     * 核心区别4: 实际应用场景对比
     * */
    @GetMapping("/supplier-vs-defer/real-world")
    public Mono<String> supplierVsDeferRealWorld() {
        System.out.println("\n========== 实际应用场景 ==========");
        
        // ✅ fromSupplier适合: 简单的延迟计算
        System.out.println("【fromSupplier适用】读取配置、获取当前时间、简单计算");
        System.out.println("  例: Mono.fromSupplier(() -> config.getValue())");
        
        // ✅ defer适合: 需要根据运行时状态做决策
        Mono<String> deferCase = Mono.defer(() -> {
            // 根据系统状态选择不同的处理方式
            boolean systemBusy = checkSystemLoad();
            System.out.println("【defer适用】系统繁忙=" + systemBusy + "，选择不同策略");
            
            if (systemBusy) {
                // 系统繁忙，返回缓存
                return Mono.just("系统繁忙 -> 返回缓存数据");
            } else {
                // 系统空闲，查询数据库
                return queryDatabase()
                    .map(result -> "系统空闲 -> 数据库查询结果: " + result);
            }
        });
        
        return deferCase;
    }
    
    private boolean checkSystemLoad() {
        return Math.random() > 0.5;
    }
    
    private Mono<String> queryDatabase() {
        return Mono.just("数据库数据");
    }

    /**
     * fromSupplier vs defer 总结
     * */
    @GetMapping("/supplier-vs-defer/summary")
    public Mono<String> supplierVsDeferSummary() {
        String summary = "\n" +
                "============ fromSupplier vs defer 核心区别 ============\n" +
                "\n" +
                "1. 【本质区别】\n" +
                "   - fromSupplier: 延迟计算\"值\"，Supplier返回T类型的值\n" +
                "   - defer: 延迟创建整个\"Mono\"，Supplier返回Mono<T>类型\n" +
                "\n" +
                "2. 【灵活性】\n" +
                "   - fromSupplier: 只能返回值或抛异常，Mono的结构是固定的\n" +
                "   - defer: 可以返回Mono.just/error/empty，可以动态构建不同的Mono\n" +
                "\n" +
                "3. 【操作符链】\n" +
                "   - fromSupplier: 操作符链在创建时就确定了，只是值延迟计算\n" +
                "     例: Mono.fromSupplier(...).map(...).filter(...)\n" +
                "         map和filter是固定的，只是Supplier的值延迟计算\n" +
                "   \n" +
                "   - defer: 可以根据订阅时的状态创建完全不同的操作符链\n" +
                "     例: Mono.defer(() -> {\n" +
                "           if (condition) return Mono.just(1).map(...);\n" +
                "           else return Mono.error(...).retry(...);\n" +
                "         })\n" +
                "\n" +
                "4. 【代码对比】\n" +
                "   - fromSupplier:\n" +
                "     Mono.fromSupplier(() -> calculateValue())\n" +
                "     // Supplier<T>: 返回值\n" +
                "   \n" +
                "   - defer:\n" +
                "     Mono.defer(() -> Mono.just(calculateValue()))\n" +
                "     // Supplier<Mono<T>>: 返回整个Mono\n" +
                "\n" +
                "5. 【适用场景】\n" +
                "   - fromSupplier:\n" +
                "     • 简单的延迟计算（读配置、获取时间、简单运算）\n" +
                "     • 值需要在订阅时才确定\n" +
                "     • 处理逻辑固定，只是值不同\n" +
                "   \n" +
                "   - defer:\n" +
                "     • 需要根据运行时状态选择不同的Mono（just/error/empty）\n" +
                "     • 需要动态构建不同的操作符链\n" +
                "     • 需要根据条件选择不同的处理逻辑\n" +
                "     • 避免重复订阅问题（包装现有的Mono）\n" +
                "\n" +
                "6. 【简单场景】\n" +
                "   在你的简单场景下（只是返回随机数），它们效果确实一样：\n" +
                "   \n" +
                "   Mono.fromSupplier(() -> random())  // 简单\n" +
                "   Mono.defer(() -> Mono.just(random()))  // 啰嗦\n" +
                "   \n" +
                "   但在复杂场景下，defer的威力才能体现！\n" +
                "\n" +
                "====================================================================\n" +
                "\n" +
                "测试建议:\n" +
                "1. /verify/supplier-vs-defer/simple - 简单场景（效果一样）\n" +
                "2. /verify/supplier-vs-defer/dynamic/error - defer动态返回error\n" +
                "3. /verify/supplier-vs-defer/dynamic/empty - defer动态返回empty\n" +
                "4. /verify/supplier-vs-defer/chain/fast - defer动态构建链\n" +
                "5. /verify/supplier-vs-defer/context - 上下文捕获差异\n";
        
        return Mono.just(summary);
    }

    // ==================== 回答用户疑问：fromSupplier的if-else vs defer ====================
    
    /**
     * 【重要】回答你的疑问：fromSupplier里用if-else vs defer
     * 
     * 你说的对！在简单场景下，fromSupplier里用if-else确实可以实现动态逻辑。
     * 但有些场景fromSupplier是做不到的，让我展示给你看！
     * */
    @GetMapping("/why-defer-needed/scenario1")
    public Mono<String> whyDeferNeeded1() {
        System.out.println("\n========== 场景1: 返回不同类型的Mono ==========");
        
        String flag = "error"; // 模拟某个条件
        
        // ❌ fromSupplier: 无法优雅地返回Mono.error或Mono.empty
        System.out.println("【fromSupplier】想返回error怎么办？");
        Mono<String> supplier = Mono.fromSupplier(() -> {
            if ("error".equals(flag)) {
                // 只能抛异常！不够优雅，而且异常处理开销大
                throw new RuntimeException("只能抛异常来表示错误");
            }
            if ("empty".equals(flag)) {
                // 只能返回null！但null的语义不明确
                return null;
            }
            return "success";
        });
        
        // ✅ defer: 可以直接返回Mono.error或Mono.empty
        System.out.println("【defer】直接返回不同类型的Mono");
        Mono<String> defer = Mono.defer(() -> {
            if ("error".equals(flag)) {
                // 直接返回error Mono，语义清晰，性能更好
                return Mono.error(new RuntimeException("优雅的错误"));
            }
            if ("empty".equals(flag)) {
                // 直接返回empty Mono，语义明确
                return Mono.empty();
            }
            return Mono.just("success");
        });
        
        return defer;
    }

    /**
     * 【核心】场景2: 包装已存在的Mono - 这是defer最常见的用途！
     * fromSupplier根本做不到这个！
     * */
    @GetMapping("/why-defer-needed/scenario2")
    public Mono<String> whyDeferNeeded2() {
        System.out.println("\n========== 场景2: 包装已存在的Mono ==========");
        
        // 假设你已经有一个返回Mono的方法（比如WebClient调用、数据库查询等）
        System.out.println("【问题】假设有个方法返回Mono<String>:");
        System.out.println("  Mono<String> callExternalApi() { ... }");
        
        // ❌ fromSupplier: 做不到！因为Supplier只能返回T，不能返回Mono<T>
        System.out.println("\n【fromSupplier】能包装这个Mono吗？");
        System.out.println("  ❌ Mono.fromSupplier(() -> callExternalApi())");
        System.out.println("  这样返回的是 Mono<Mono<String>>，类型错误！");
        
        // ✅ defer: 完美包装！
        System.out.println("\n【defer】完美包装:");
        Mono<String> defer = Mono.defer(() -> {
            System.out.println("  每次订阅都会重新调用API");
            return callExternalApi(); // 返回Mono<String>
        });
        
        System.out.println("\n第一次订阅:");
        defer.subscribe(v -> System.out.println("  结果: " + v));
        
        System.out.println("\n第二次订阅:");
        defer.subscribe(v -> System.out.println("  结果: " + v));
        
        return Mono.just("defer可以包装已存在的Mono，fromSupplier做不到！");
    }
    
    // 模拟外部API调用
    private Mono<String> callExternalApi() {
        return Mono.just("API返回: " + System.currentTimeMillis());
    }

    /**
     * 【核心】场景3: 不同的操作符链（流级别的差异）
     * 你在fromSupplier里只能做业务逻辑判断，但不能改变"流"本身的行为！
     * */
    @GetMapping("/why-defer-needed/scenario3/{type}")
    public Mono<String> whyDeferNeeded3(@PathVariable String type) {
        System.out.println("\n========== 场景3: 流级别的差异 ==========");
        
        // ❌ fromSupplier: 操作符是固定的！
        System.out.println("【fromSupplier】操作符链在创建时就固定了:");
        Mono<String> supplier = Mono.fromSupplier(() -> {
            if ("retry".equals(type)) {
                // 你在这里用if-else只能改变返回值
                return "需要重试的场景";
            } else {
                return "不需要重试的场景";
            }
        })
        .retry(3)  // ❌ 这个retry对所有情况都生效！无法根据type动态决定
        .timeout(Duration.ofSeconds(5)); // ❌ 这个timeout对所有情况都生效！
        
        System.out.println("  问题: retry和timeout对所有情况都生效，无法根据type动态选择");
        
        // ✅ defer: 可以根据type选择完全不同的流处理策略！
        System.out.println("\n【defer】可以动态选择流处理策略:");
        Mono<String> defer = Mono.defer(() -> {
            if ("retry".equals(type)) {
                System.out.println("  选择: 带重试的流");
                return Mono.just("重要操作")
                    .retry(3)  // 只有这个分支有retry
                    .timeout(Duration.ofSeconds(10)); // 重要操作给更长超时
            } else if ("fast".equals(type)) {
                System.out.println("  选择: 快速响应流");
                return Mono.just("快速操作")
                    .timeout(Duration.ofSeconds(1)); // 快速操作短超时，无retry
            } else {
                System.out.println("  选择: 普通流");
                return Mono.just("普通操作"); // 无retry，无timeout
            }
        });
        
        return defer;
    }

    /**
     * 【核心】场景4: 不同的数据源（返回的是不同的Mono流）
     * 这个fromSupplier根本做不到！
     * */
    @GetMapping("/why-defer-needed/scenario4/{source}")
    public Mono<String> whyDeferNeeded4(@PathVariable String source) {
        System.out.println("\n========== 场景4: 不同的数据源 ==========");
        
        // ❌ fromSupplier: 只能在一个方法里完成所有逻辑
        System.out.println("【fromSupplier】只能这样:");
        Mono<String> supplier = Mono.fromSupplier(() -> {
            if ("database".equals(source)) {
                // 你必须在这里阻塞等待数据库结果！
                // 因为Supplier必须立即返回String，不能返回Mono
                return "只能同步调用数据库"; // ❌ 失去了异步优势！
            } else {
                return "只能同步调用缓存"; // ❌ 失去了异步优势！
            }
        });
        
        // ✅ defer: 可以返回不同的异步Mono流！
        System.out.println("\n【defer】可以返回不同的异步流:");
        Mono<String> defer = Mono.defer(() -> {
            if ("database".equals(source)) {
                System.out.println("  从数据库异步查询");
                return queryDatabase(); // 返回Mono<String>，保持异步
            } else if ("cache".equals(source)) {
                System.out.println("  从缓存异步查询");
                return queryCache(); // 返回Mono<String>，保持异步
            } else if ("api".equals(source)) {
                System.out.println("  从外部API异步查询");
                return callExternalApi(); // 返回Mono<String>，保持异步
            } else {
                return Mono.just("默认值");
            }
        });
        
        return defer;
    }
    
    private Mono<String> queryCache() {
        return Mono.just("缓存数据").delayElement(Duration.ofMillis(100));
    }

    /**
     * 【核心】场景5: 链式调用不同的异步操作
     * fromSupplier在这种场景下完全无能为力！
     * */
    @GetMapping("/why-defer-needed/scenario5/{flow}")
    public Mono<String> whyDeferNeeded5(@PathVariable String flow) {
        System.out.println("\n========== 场景5: 不同的异步操作链 ==========");
        
        // ❌ fromSupplier: 无法处理不同的异步操作链
        System.out.println("【fromSupplier】无法做到:");
        System.out.println("  - 想根据flow选择: 先查数据库再调API");
        System.out.println("  - 或者: 先调API再更新缓存");
        System.out.println("  因为Supplier只能返回最终值，不能返回异步操作链！");
        
        // ✅ defer: 可以返回完全不同的异步操作链！
        System.out.println("\n【defer】可以返回不同的异步操作链:");
        Mono<String> defer = Mono.defer(() -> {
            if ("db-then-api".equals(flow)) {
                System.out.println("  流程: 数据库 -> API");
                return queryDatabase()
                    .flatMap(dbResult -> {
                        System.out.println("  DB结果: " + dbResult);
                        return callExternalApi();
                    });
            } else if ("api-then-cache".equals(flow)) {
                System.out.println("  流程: API -> 缓存更新");
                return callExternalApi()
                    .flatMap(apiResult -> {
                        System.out.println("  API结果: " + apiResult);
                        return updateCache(apiResult);
                    });
            } else if ("parallel".equals(flow)) {
                System.out.println("  流程: 并行查询多个源");
                return Mono.zip(queryDatabase(), queryCache(), callExternalApi())
                    .map(tuple -> String.format("DB:%s, Cache:%s, API:%s", 
                        tuple.getT1(), tuple.getT2(), tuple.getT3()));
            } else {
                return Mono.just("默认流程");
            }
        });
        
        return defer;
    }
    
    private Mono<String> updateCache(String value) {
        return Mono.just("已更新缓存: " + value);
    }

    /**
     * 总结：什么时候fromSupplier的if-else不够用？
     * */
    @GetMapping("/why-defer-needed/summary")
    public Mono<String> whyDeferSummary() {
        String summary = "\n" +
                "========== 什么时候fromSupplier的if-else不够用？ ==========\n" +
                "\n" +
                "你说得对！很多简单场景下，fromSupplier里用if-else确实够用：\n" +
                "  Mono.fromSupplier(() -> {\n" +
                "      if (condition) return \"A\";\n" +
                "      else return \"B\";\n" +
                "  })\n" +
                "\n" +
                "但以下场景fromSupplier做不到，必须用defer：\n" +
                "\n" +
                "1. 【包装已存在的Mono】★★★ 最常见的用途！\n" +
                "   问题: 你有个方法返回Mono<T>，想延迟调用它\n" +
                "   \n" +
                "   ❌ Mono.fromSupplier(() -> someMethod())  \n" +
                "      如果someMethod()返回Mono<String>，这会变成Mono<Mono<String>>\n" +
                "   \n" +
                "   ✅ Mono.defer(() -> someMethod())\n" +
                "      完美！每次订阅都会重新调用someMethod()\n" +
                "\n" +
                "2. 【不同的数据源】\n" +
                "   问题: 根据条件从数据库/缓存/API获取数据，它们都返回Mono\n" +
                "   \n" +
                "   ❌ fromSupplier: 必须同步等待结果，失去异步优势\n" +
                "   ✅ defer: 直接返回不同的Mono流，保持异步\n" +
                "\n" +
                "3. 【不同的操作符链】\n" +
                "   问题: 根据场景需要不同的retry/timeout/cache等策略\n" +
                "   \n" +
                "   ❌ fromSupplier: 操作符链在创建时固定，无法动态改变\n" +
                "   ✅ defer: 可以返回带有不同操作符的Mono\n" +
                "\n" +
                "4. 【不同的异步操作链】\n" +
                "   问题: 需要不同的flatMap/zip等异步组合\n" +
                "   \n" +
                "   ❌ fromSupplier: 只能返回最终值，不能返回异步操作链\n" +
                "   ✅ defer: 可以返回完全不同的异步操作流程\n" +
                "\n" +
                "5. 【优雅地返回error/empty】\n" +
                "   问题: 某些条件下需要返回Mono.error或Mono.empty\n" +
                "   \n" +
                "   ❌ fromSupplier: 只能抛异常或返回null（不优雅）\n" +
                "   ✅ defer: 直接返回Mono.error/Mono.empty（语义清晰）\n" +
                "\n" +
                "==============================================================\n" +
                "\n" +
                "【关键理解】\n" +
                "- fromSupplier: 延迟计算\"值\"，返回类型是 T\n" +
                "  适合: 简单的if-else逻辑，最后返回一个值\n" +
                "\n" +
                "- defer: 延迟创建整个\"Mono流\"，返回类型是 Mono<T>\n" +
                "  适合: 需要返回不同的Mono、不同的操作符链、包装已存在的Mono\n" +
                "\n" +
                "【简单判断法则】\n" +
                "- 如果你的if-else最后返回的是\"值\" -> 用fromSupplier\n" +
                "- 如果你的if-else最后返回的是\"Mono\" -> 必须用defer\n" +
                "\n" +
                "测试这些场景:\n" +
                "1. /verify/why-defer-needed/scenario2 - 包装Mono（最常见）\n" +
                "2. /verify/why-defer-needed/scenario3/retry - 不同的流策略\n" +
                "3. /verify/why-defer-needed/scenario4/database - 不同的数据源\n" +
                "4. /verify/why-defer-needed/scenario5/db-then-api - 异步操作链\n";
        
        return Mono.just(summary);
    }

    // ==================== create vs fromSupplier 区别演示 ====================

    /**
     * 总结对比: create vs fromSupplier
     * 访问这个接口查看详细对比说明
     * */
    @GetMapping("/difference/summary")
    public Mono<String> differenceSummary() {
        String summary = "\n" +
                "============ Mono.create() vs Mono.fromSupplier() 核心区别 ============\n" +
                "\n" +
                "1. 【执行模型】\n" +
                "   - fromSupplier: 同步执行，Supplier.get()必须立即返回值\n" +
                "   - create: 异步支持，MonoSink可以在任意时间点发射数据\n" +
                "\n" +
                "2. 【适用场景】\n" +
                "   - fromSupplier: 延迟计算（订阅时才计算），但必须是同步计算\n" +
                "     例如: 读取配置、简单计算、获取当前时间等\n" +
                "   \n" +
                "   - create: 异步回调、不确定何时完成的操作\n" +
                "     例如: 异步HTTP请求、消息队列回调、定时器、异步数据库操作等\n" +
                "\n" +
                "3. 【代码对比】\n" +
                "   - fromSupplier:\n" +
                "     Mono.fromSupplier(() -> {\n" +
                "         return \"必须立即返回\";  // ✅ 同步返回\n" +
                "     })\n" +
                "   \n" +
                "   - create:\n" +
                "     Mono.create(sink -> {\n" +
                "         asyncOperation(() -> {\n" +
                "             sink.success(\"可以延迟发射\");  // ✅ 异步发射\n" +
                "         });\n" +
                "     })\n" +
                "\n" +
                "4. 【线程行为】\n" +
                "   - fromSupplier: Supplier在订阅线程上执行，阻塞订阅线程直到返回\n" +
                "   - create: Consumer在订阅线程上执行，但sink.success()可以在任意线程调用\n" +
                "\n" +
                "5. 【典型错误】\n" +
                "   - fromSupplier中启动异步任务后立即返回 ❌\n" +
                "     结果: 异步任务的结果无法传递给订阅者\n" +
                "   \n" +
                "   - create中正确处理异步任务 ✅\n" +
                "     结果: 异步任务完成后通过sink.success()发射数据\n" +
                "\n" +
                "====================================================================\n" +
                "\n" +
                "测试建议:\n" +
                "1. 访问 /verify/difference/supplier - 看同步执行\n" +
                "2. 访问 /verify/difference/create - 看异步执行\n" +
                "3. 访问 /verify/difference/supplier-async-wrong - 看错误用法\n" +
                "4. 访问 /verify/difference/create-async-right - 看正确用法\n" +
                "5. 访问 /verify/difference/callback-scenario - 看真实场景\n";
        
        return Mono.just(summary);
    }

}
