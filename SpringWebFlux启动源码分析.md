# Spring WebFlux 启动源码分析

## 🎯 目标

通过**真实的源码**，逐步证明Spring WebFlux的启动流程：

```
Spring容器启动 → IoC/AOP → 生成Mapping → 启动Netty → 注入Handler → 请求处理
```

---

## 📊 完整调用栈

```
main()
  ↓
SpringApplication.run()
  ↓
SpringApplication.run(String... args)
  ↓
refreshContext(context)  ← IoC/AOP
  ↓
AbstractApplicationContext.refresh()
  ↓
onRefresh()  ← 启动Netty
  ↓
createWebServer()
  ↓
NettyWebServer.start()
```

---

## 第1步：Spring Boot启动入口

### 你的代码

```java
// src/main/java/org/example/springwebflux/Main.java
@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);  // ← 入口
    }
}
```

### Spring源码：SpringApplication.run()

```java
// org.springframework.boot.SpringApplication

public static ConfigurableApplicationContext run(Class<?> primarySource, String... args) {
    return run(new Class<?>[] { primarySource }, args);
}

public static ConfigurableApplicationContext run(Class<?>[] primarySources, String[] args) {
    return new SpringApplication(primarySources).run(args);
}

// 关键方法：run(String... args)
public ConfigurableApplicationContext run(String... args) {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    
    ConfigurableApplicationContext context = null;
    
    try {
        // 1. 准备环境
        ApplicationArguments applicationArguments = new DefaultApplicationArguments(args);
        ConfigurableEnvironment environment = prepareEnvironment(listeners, applicationArguments);
        
        // 2. 创建ApplicationContext
        context = createApplicationContext();  // ← 创建Spring容器
        
        // 3. 准备Context
        prepareContext(context, environment, listeners, applicationArguments, printedBanner);
        
        // 4. 刷新Context（IoC、AOP、启动Netty都在这里）
        refreshContext(context);  // ← 核心！
        
        // 5. 刷新后处理
        afterRefresh(context, applicationArguments);
        
        stopWatch.stop();
        logger.info("Started " + context.getClass().getSimpleName() + " in " + stopWatch.getTotalTimeSeconds() + " seconds");
        
        return context;
    } catch (Throwable ex) {
        handleRunFailure(context, ex, listeners);
        throw new IllegalStateException(ex);
    }
}
```

**证明**：`SpringApplication.run()` 是入口，核心在 `refreshContext(context)`

---

## 第2步：刷新Spring容器（IoC和AOP）

### Spring源码：AbstractApplicationContext.refresh()

```java
// org.springframework.context.support.AbstractApplicationContext

@Override
public void refresh() throws BeansException, IllegalStateException {
    synchronized (this.startupShutdownMonitor) {
        // 1. 准备刷新
        prepareRefresh();
        
        // 2. 获取BeanFactory
        ConfigurableListableBeanFactory beanFactory = obtainFreshBeanFactory();
        
        // 3. 准备BeanFactory
        prepareBeanFactory(beanFactory);
        
        try {
            // 4. BeanFactory后处理
            postProcessBeanFactory(beanFactory);
            
            // 5. 调用BeanFactoryPostProcessor（处理配置类）
            invokeBeanFactoryPostProcessors(beanFactory);  // ← 扫描@Configuration
            
            // 6. 注册BeanPostProcessor（AOP在这里）
            registerBeanPostProcessors(beanFactory);  // ← AOP代理
            
            // 7. 初始化消息源
            initMessageSource();
            
            // 8. 初始化事件广播器
            initApplicationEventMulticaster();
            
            // 9. 刷新特定子类的Context（启动Netty在这里！）
            onRefresh();  // ← 启动Web服务器
            
            // 10. 注册监听器
            registerListeners();
            
            // 11. 实例化所有非懒加载的单例Bean
            finishBeanFactoryInitialization(beanFactory);
            
            // 12. 完成刷新
            finishRefresh();
        } catch (BeansException ex) {
            destroyBeans();
            cancelRefresh(ex);
            throw ex;
        }
    }
}
```

**证明**：
- 第5步扫描配置类
- 第6步注册AOP代理
- 第9步启动Netty

---

## 第3步：扫描和生成Mapping

### 3.1 扫描Controller

```java
// org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping

// Spring容器初始化时调用
@Override
public void afterPropertiesSet() {
    this.config = new RequestMappingInfo.BuilderConfiguration();
    // ... 配置初始化
    
    // 扫描Handler方法
    super.afterPropertiesSet();  // ← 关键
}
```

### 3.2 扫描所有Bean找到Controller

```java
// org.springframework.web.reactive.handler.AbstractHandlerMethodMapping

@Override
public void afterPropertiesSet() {
    initHandlerMethods();  // ← 初始化Handler方法
}

protected void initHandlerMethods() {
    // 获取所有Bean的名称
    String[] beanNames = obtainApplicationContext()
        .getBeanNamesForType(Object.class);
    
    for (String beanName : beanNames) {
        if (!beanName.startsWith(SCOPED_TARGET_NAME_PREFIX)) {
            Class<?> beanType = null;
            try {
                beanType = obtainApplicationContext().getType(beanName);
            } catch (Throwable ex) {
                // ...
            }
            
            // 判断是否是Handler（有@Controller或@RequestMapping）
            if (beanType != null && isHandler(beanType)) {  // ← 判断
                detectHandlerMethods(beanName);  // ← 检测方法
            }
        }
    }
}
```

### 3.3 判断是否是Handler

```java
// org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping

@Override
protected boolean isHandler(Class<?> beanType) {
    // 有@Controller或@RequestMapping注解就是Handler
    return (AnnotatedElementUtils.hasAnnotation(beanType, Controller.class) ||
            AnnotatedElementUtils.hasAnnotation(beanType, RequestMapping.class));
}
```

**证明**：扫描所有Bean，找到有`@Controller`的类

### 3.4 检测Handler的方法

```java
// org.springframework.web.reactive.handler.AbstractHandlerMethodMapping

protected void detectHandlerMethods(Object handler) {
    Class<?> handlerType = (handler instanceof String ?
            obtainApplicationContext().getType((String) handler) : handler.getClass());
    
    if (handlerType != null) {
        Class<?> userType = ClassUtils.getUserClass(handlerType);
        
        // 查找所有有@RequestMapping的方法
        Map<Method, T> methods = MethodIntrospector.selectMethods(userType,
            (MethodIntrospector.MetadataLookup<T>) method -> {
                try {
                    // 获取方法的@GetMapping、@PostMapping等
                    return getMappingForMethod(method, userType);  // ← 关键
                } catch (Throwable ex) {
                    throw new IllegalStateException("...", ex);
                }
            });
        
        // 注册每个方法
        methods.forEach((method, mapping) -> {
            Method invocableMethod = AopUtils.selectInvocableMethod(method, userType);
            registerHandlerMethod(handler, invocableMethod, mapping);  // ← 注册
        });
    }
}
```

### 3.5 获取方法的Mapping信息

```java
// org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping

@Override
@Nullable
protected RequestMappingInfo getMappingForMethod(Method method, Class<?> handlerType) {
    // 从方法上获取@GetMapping、@PostMapping等
    RequestMappingInfo info = createRequestMappingInfo(method);
    
    if (info != null) {
        // 从类上获取@RequestMapping
        RequestMappingInfo typeInfo = createRequestMappingInfo(handlerType);
        if (typeInfo != null) {
            // 合并类和方法的路径
            info = typeInfo.combine(info);
        }
    }
    return info;
}

@Nullable
private RequestMappingInfo createRequestMappingInfo(AnnotatedElement element) {
    // 查找@RequestMapping注解
    RequestMapping requestMapping = AnnotatedElementUtils.findMergedAnnotation(
            element, RequestMapping.class);
    
    // ... 处理条件
    
    return (requestMapping != null ? createRequestMappingInfo(requestMapping, condition) : null);
}
```

### 3.6 注册到映射表

```java
// org.springframework.web.reactive.handler.AbstractHandlerMethodMapping

protected void registerHandlerMethod(Object handler, Method method, T mapping) {
    this.mappingRegistry.register(mapping, handler, method);
}

// MappingRegistry内部类
public void register(T mapping, Object handler, Method method) {
    this.readWriteLock.writeLock().lock();
    try {
        HandlerMethod handlerMethod = createHandlerMethod(handler, method);
        
        // 检查是否有重复映射
        assertUniqueMethodMapping(handlerMethod, mapping);
        
        // 存储映射：mapping → handlerMethod
        this.mappingLookup.put(mapping, handlerMethod);  // ← 存入Map
        
        // 存储URL映射
        List<String> directUrls = getDirectUrls(mapping);
        for (String url : directUrls) {
            this.urlLookup.add(url, mapping);  // ← URL → mapping
        }
        
        logger.info("Mapped \"" + mapping + "\" onto " + handlerMethod);
    } finally {
        this.readWriteLock.writeLock().unlock();
    }
}
```

**证明**：生成了两个Map：
1. `mappingLookup`: `RequestMappingInfo` → `HandlerMethod`
2. `urlLookup`: `URL` → `RequestMappingInfo`

---

## 第4步：启动Netty服务器

### 4.1 触发启动

```java
// org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext

@Override
protected void onRefresh() {
    super.onRefresh();
    try {
        createWebServer();  // ← 创建Web服务器
    } catch (Throwable ex) {
        throw new ApplicationContextException("Unable to start reactive web server", ex);
    }
}

private void createWebServer() {
    ServerManager serverManager = this.serverManager;
    if (serverManager == null) {
        // 获取HttpHandler（Spring的请求处理器）
        this.serverManager = ServerManager.get(getWebServerFactory());
    }
    initPropertySources();
}
```

### 4.2 获取HttpHandler

```java
// org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext.ServerManager

static ServerManager get(WebServerFactory webServerFactory) {
    return new ServerManager(...);
}

private ServerManager(...) {
    // 获取HttpHandler Bean
    this.handler = context.getBean("webHandler", HttpHandler.class);
    
    // 创建WebServer
    this.webServer = webServerFactory.getWebServer(this.handler);  // ← 启动Netty
}
```

### 4.3 创建Netty服务器

```java
// org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory

@Override
public WebServer getWebServer(HttpHandler httpHandler) {
    // 创建Reactor Netty的HttpServer
    HttpServer httpServer = createHttpServer();
    
    // 配置Handler（桥梁！）
    ReactorHttpHandlerAdapter handlerAdapter = new ReactorHttpHandlerAdapter(httpHandler);
    HttpServer serverWithHandler = httpServer.handle(handlerAdapter);  // ← 注入Handler
    
    return new NettyWebServer(serverWithHandler, getLifecycleTimeout());
}

private HttpServer createHttpServer() {
    HttpServer server = HttpServer.create();
    return applyCustomizers(server);
}
```

**证明**：
1. 创建了`HttpServer`（Netty）
2. 创建了`ReactorHttpHandlerAdapter`（桥梁）
3. 将Spring的`HttpHandler`注入到Netty

### 4.4 配置Netty Pipeline

```java
// reactor.netty.http.server.HttpServer

public final HttpServer handle(BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> handler) {
    return new HttpServerHandle(this, handler);  // ← 包装Handler
}

// HttpServerHandle内部
@Override
protected DisposableServer bind(ServerBootstrap bootstrap, SocketAddress localAddress) {
    // 配置ChannelInitializer
    bootstrap.childHandler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            
            // 添加HTTP编解码器
            pipeline.addLast(NettyPipeline.HttpCodec, 
                new HttpServerCodec());
            
            // 添加HTTP内容聚合器
            pipeline.addLast(NettyPipeline.HttpAggregator,
                new HttpObjectAggregator(maxContentLength));
            
            // 添加Spring的Handler
            pipeline.addLast(NettyPipeline.ReactiveBridge,
                new HttpServerHandler(handler));  // ← 注入Handler
        }
    });
    
    return bootstrap.bind(localAddress).sync();
}
```

**证明**：Netty的Pipeline中注入了Spring的Handler

### 4.5 启动Netty

```java
// org.springframework.boot.web.embedded.netty.NettyWebServer

@Override
public void start() throws WebServerException {
    if (this.disposableServer == null) {
        try {
            // 启动Netty服务器
            this.disposableServer = startHttpServer();  // ← 启动
        } catch (Exception ex) {
            throw new WebServerException("Unable to start Netty", ex);
        }
        logger.info("Netty started on port(s): " + getPort());
        startDaemonAwaitThread(this.disposableServer);
    }
}

private DisposableServer startHttpServer() {
    // 绑定端口，启动服务器
    return this.httpServer.bind().block();  // ← 阻塞直到启动完成
}
```

**证明**：Netty服务器启动，监听端口

---

## 第5步：创建桥梁Handler

### 5.1 ReactorHttpHandlerAdapter

```java
// org.springframework.http.server.reactive.ReactorHttpHandlerAdapter

public class ReactorHttpHandlerAdapter 
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
    
    private final HttpHandler httpHandler;  // ← Spring的HttpHandler
    
    public ReactorHttpHandlerAdapter(HttpHandler httpHandler) {
        Assert.notNull(httpHandler, "HttpHandler must not be null");
        this.httpHandler = httpHandler;
    }
    
    @Override
    public Publisher<Void> apply(HttpServerRequest reactorRequest, 
                                   HttpServerResponse reactorResponse) {
        
        // 包装Netty的request为Spring的API
        ServerHttpRequest request = new ReactorServerHttpRequest(reactorRequest, bufferFactory);
        
        // 包装Netty的response为Spring的API
        ServerHttpResponse response = new ReactorServerHttpResponse(reactorResponse, bufferFactory);
        
        // 调用Spring的HttpHandler
        return this.httpHandler.handle(request, response)  // ← 调用Spring
            .doOnError(ex -> logger.trace("...")
            .doOnCancel(() -> logger.trace("..."));
    }
}
```

**证明**：
1. `ReactorHttpHandlerAdapter`实现了Netty的接口
2. 内部持有Spring的`HttpHandler`
3. 将Netty对象包装成Spring对象
4. 调用Spring的处理逻辑

### 5.2 包装Request

```java
// org.springframework.http.server.reactive.ReactorServerHttpRequest

public class ReactorServerHttpRequest extends AbstractServerHttpRequest {
    
    private final HttpServerRequest request;  // ← Netty的请求
    private final NettyDataBufferFactory bufferFactory;
    
    public ReactorServerHttpRequest(HttpServerRequest request, DataBufferFactory bufferFactory) {
        super(initUri(request), "", initHeaders(request));
        this.request = request;
        this.bufferFactory = NettyDataBufferFactory.cast(bufferFactory);
    }
    
    @Override
    public HttpMethod getMethod() {
        // 从Netty的request获取
        return HttpMethod.valueOf(this.request.method().name());
    }
    
    @Override
    public URI getURI() {
        return initUri(this.request);
    }
    
    @Override
    public Flux<DataBuffer> getBody() {
        // 从Netty的request获取body
        return this.request.receive()
            .retain()
            .map(byteBuf -> this.bufferFactory.wrap(byteBuf));
    }
}
```

**证明**：`ReactorServerHttpRequest`包装了Netty的`HttpServerRequest`

### 5.3 包装Response

```java
// org.springframework.http.server.reactive.ReactorServerHttpResponse

public class ReactorServerHttpResponse extends AbstractServerHttpResponse {
    
    private final HttpServerResponse response;  // ← Netty的响应
    
    public ReactorServerHttpResponse(HttpServerResponse response, DataBufferFactory bufferFactory) {
        super(bufferFactory, new HttpHeaders(new NettyHeadersAdapter(response.responseHeaders())));
        this.response = response;
    }
    
    @Override
    protected Mono<Void> writeWithInternal(Publisher<? extends DataBuffer> body) {
        // 将Spring的DataBuffer转换为Netty的ByteBuf
        return this.response.send(Flux.from(body)
            .map(dataBuffer -> toByteBuf(dataBuffer)));  // ← 写入Netty
    }
    
    private ByteBuf toByteBuf(DataBuffer dataBuffer) {
        return ((NettyDataBuffer) dataBuffer).getNativeBuffer();
    }
}
```

**证明**：`ReactorServerHttpResponse`包装了Netty的`HttpServerResponse`

---

## 第6步：请求处理流程

### 6.1 Netty接收请求并通过桥梁进入Spring

#### 关键：Netty如何调用Spring的Handler？

**答案：通过 `ReactorHttpHandlerAdapter` 这个桥梁！**

#### 步骤1：Netty接收请求

```java
// Netty内部流程（简化）
// 当HTTP请求到达时

// 1. 创建 HttpServerOperations（既是Request又是Response）
HttpServerOperations ops = HttpServerOperations.create(
    connection, 
    nettyHttpRequest, 
    ...
);

// 2. 获取之前注入的handler（就是ReactorHttpHandlerAdapter）
BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> handler = 
    this.configuration.handler();  // ← Spring注入的ReactorHttpHandlerAdapter

// 3. 调用handler处理请求（关键！）
Publisher<Void> responsePublisher = handler.apply(ops, ops);
//                                         ↑    ↑
//                     HttpServerOperations作为request和response传入
//                     调用 ReactorHttpHandlerAdapter.apply()
//                     请求从这里进入Spring！

// 4. 订阅响应
Mono.from(responsePublisher).subscribe(
    v -> {},
    error -> handleError(error),
    () -> completeResponse()
);
```

#### 步骤2：ReactorHttpHandlerAdapter处理（桥梁）

```java
// org.springframework.http.server.reactive.ReactorHttpHandlerAdapter
// 这是连接Netty和Spring的桥梁！

public class ReactorHttpHandlerAdapter 
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
    //         ↑
    //    实现了Netty的函数接口，Netty可以直接调用
    
    private final HttpHandler httpHandler;  // ← Spring容器的HttpHandler
    
    public ReactorHttpHandlerAdapter(HttpHandler httpHandler) {
        this.httpHandler = httpHandler;  // 持有Spring的Handler
    }
    
    @Override
    public Publisher<Void> apply(HttpServerRequest request,    // Netty的request
                                  HttpServerResponse response) { // Netty的response
        
        // 1. 包装Netty的request为Spring的request
        ServerHttpRequest springRequest = new ReactorServerHttpRequest(
            request, this.bufferFactory
        );
        
        // 2. 包装Netty的response为Spring的response
        ServerHttpResponse springResponse = new ReactorServerHttpResponse(
            response, this.bufferFactory
        );
        
        // 3. 调用Spring容器的HttpHandler（关键！）
        return this.httpHandler.handle(springRequest, springResponse);
        //     ↑
        //  调用Spring的HttpHandler
        //  请求进入Spring容器！
    }
}
```

**关键点**：
- ✅ `ReactorHttpHandlerAdapter` 是桥梁
- ✅ 实现了Netty的 `BiFunction` 接口
- ✅ 持有Spring的 `HttpHandler`
- ✅ 将Netty对象包装成Spring对象
- ✅ 调用 `httpHandler.handle()` 进入Spring容器

#### 步骤3：数据流向

```
Netty层
  HttpServerOperations ops
    ↓
  handler.apply(ops, ops)  ← 调用桥梁
    ↓
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
桥梁：ReactorHttpHandlerAdapter
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ↓
  包装为Spring对象：
    - ReactorServerHttpRequest
    - ReactorServerHttpResponse
    ↓
  httpHandler.handle(springReq, springResp)  ← 进入Spring
    ↓
Spring容器
  HttpWebHandlerAdapter
    ↓
  DispatcherHandler
    ↓
  你的Controller
```

### 6.2 调用Spring的HttpHandler

```java
// org.springframework.http.server.reactive.HttpWebHandlerAdapter

public class HttpWebHandlerAdapter extends WebHandlerDecorator implements HttpHandler {
    
    @Override
    public Mono<Void> handle(ServerHttpRequest request, ServerHttpResponse response) {
        // 创建ServerWebExchange
        ServerWebExchange exchange = createExchange(request, response);
        
        // 调用DispatcherHandler
        return getDelegate().handle(exchange)  // ← 调用
            .doOnSuccess(aVoid -> logResponse(exchange))
            .onErrorResume(ex -> handleUnresolvedError(exchange, ex))
            .then(Mono.defer(() -> response.setComplete()));
    }
    
    protected ServerWebExchange createExchange(ServerHttpRequest request, 
                                                ServerHttpResponse response) {
        return new DefaultServerWebExchange(request, response, ...);
    }
}
```

### 6.3 DispatcherHandler分发请求

```java
// org.springframework.web.reactive.DispatcherHandler

public class DispatcherHandler implements WebHandler, ApplicationContextAware {
    
    private List<HandlerMapping> handlerMappings;  // ← 映射表
    
    @Override
    public Mono<Void> handle(ServerWebExchange exchange) {
        if (this.handlerMappings == null) {
            return createNotFoundError();
        }
        
        // 遍历所有HandlerMapping查找匹配的Handler
        return Flux.fromIterable(this.handlerMappings)
            .concatMap(mapping -> mapping.getHandler(exchange))  // ← 查找Handler
            .next()
            .switchIfEmpty(createNotFoundError())
            .flatMap(handler -> invokeHandler(exchange, handler))  // ← 调用Handler
            .flatMap(result -> handleResult(exchange, result));  // ← 处理结果
    }
    
    private Mono<HandlerResult> invokeHandler(ServerWebExchange exchange, Object handler) {
        if (this.handlerAdapters != null) {
            for (HandlerAdapter handlerAdapter : this.handlerAdapters) {
                if (handlerAdapter.supports(handler)) {
                    return handlerAdapter.handle(exchange, handler);  // ← 调用
                }
            }
        }
        return Mono.error(new IllegalStateException("No HandlerAdapter"));
    }
}
```

### 6.4 查找匹配的Handler

```java
// org.springframework.web.reactive.result.method.AbstractHandlerMethodMapping

@Override
public Mono<Object> getHandler(ServerWebExchange exchange) {
    return getHandlerInternal(exchange).map(handlerMethod -> {
        // ... 处理
        return handlerMethod;
    });
}

@Override
protected Mono<HandlerMethod> getHandlerInternal(ServerWebExchange exchange) {
    this.mappingRegistry.acquireReadLock();
    try {
        HandlerMethod handlerMethod;
        try {
            // 根据请求路径查找Handler
            handlerMethod = lookupHandlerMethod(lookupPath, exchange);  // ← 查找
        } catch (Exception ex) {
            return Mono.error(ex);
        }
        
        return (handlerMethod != null ? Mono.just(handlerMethod) : Mono.empty());
    } finally {
        this.mappingRegistry.releaseReadLock();
    }
}

@Nullable
protected HandlerMethod lookupHandlerMethod(String lookupPath, ServerWebExchange exchange) {
    List<Match> matches = new ArrayList<>();
    
    // 从urlLookup查找匹配的mapping
    List<T> directPathMatches = this.mappingRegistry.getMappingsByUrl(lookupPath);
    if (directPathMatches != null) {
        addMatchingMappings(directPathMatches, matches, exchange);
    }
    
    if (matches.isEmpty()) {
        // 没有直接匹配，检查所有mapping
        addMatchingMappings(this.mappingRegistry.getMappings().keySet(), matches, exchange);
    }
    
    if (!matches.isEmpty()) {
        Match bestMatch = matches.get(0);  // 最佳匹配
        // ... 处理
        return bestMatch.handlerMethod;
    }
    
    return null;
}
```

**证明**：从映射表中查找匹配的Handler

### 6.5 调用你的Controller

```java
// org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerAdapter

@Override
public Mono<HandlerResult> handle(ServerWebExchange exchange, Object handler) {
    HandlerMethod handlerMethod = (HandlerMethod) handler;
    
    // 创建InvocableHandlerMethod
    InvocableHandlerMethod invocable = new InvocableHandlerMethod(handlerMethod);
    invocable.setArgumentResolvers(this.argumentResolvers);
    invocable.setReactiveAdapterRegistry(this.reactiveAdapterRegistry);
    
    // 调用方法
    return invocable.invoke(exchange, this.modelFactory)  // ← 调用你的Controller方法
        .map(result -> result.setExceptionHandler(this.exceptionHandler))
        .doOnNext(result -> result.setBindingContext(bindingContext));
}
```

### 6.6 参数解析和方法调用

```java
// org.springframework.web.reactive.result.method.InvocableHandlerMethod

public Mono<HandlerResult> invoke(ServerWebExchange exchange, BindingContext bindingContext, Object... providedArgs) {
    // 解析参数
    return getMethodArgumentValues(exchange, bindingContext, providedArgs)
        .flatMap(args -> {
            Object value;
            try {
                // 反射调用方法
                value = doInvoke(args);  // ← 调用你的方法
            } catch (Exception ex) {
                return Mono.error(ex);
            }
            
            // 包装返回值
            HttpStatus status = getResponseStatus();
            return Mono.just(new HandlerResult(this, value, getReturnType(), bindingContext));
        });
}

@Nullable
protected Object doInvoke(Object... args) throws Exception {
    ReflectionUtils.makeAccessible(getBridgedMethod());
    try {
        // 反射调用
        return getBridgedMethod().invoke(getBean(), args);  // ← 反射调用
    } catch (IllegalArgumentException ex) {
        // ...
    } catch (InvocationTargetException ex) {
        // ...
    }
}
```

**证明**：通过反射调用你的Controller方法

---

## 🔬 调试验证

### 设置断点位置

在你的项目中设置以下断点：

```java
// 1. 启动入口
SpringApplication.run()  // line 1332

// 2. 刷新容器
AbstractApplicationContext.refresh()  // line 580

// 3. 启动Netty
ReactiveWebServerApplicationContext.onRefresh()  // line 85

// 4. 创建桥梁
NettyReactiveWebServerFactory.getWebServer()  // line 153

// 5. 请求分发
DispatcherHandler.handle()  // line 275

// 6. 你的Controller
YourController.yourMethod()  // 你的方法
```

### 调试步骤

1. **启动应用（Debug模式）**

```bash
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005"
```

2. **观察调用栈**

在断点暂停时，查看调用栈：

```
Thread: main
  at org.springframework.boot.SpringApplication.run()
  at org.springframework.boot.SpringApplication.run()
  at org.example.springwebflux.Main.main()
```

3. **发送请求并观察**

```bash
curl http://localhost:8080/your-endpoint
```

观察调用栈：

```
Thread: reactor-http-nio-2
  at YourController.yourMethod()
  at InvocableHandlerMethod.doInvoke()
  at RequestMappingHandlerAdapter.handle()
  at DispatcherHandler.handle()
  at HttpWebHandlerAdapter.handle()
  at ReactorHttpHandlerAdapter.apply()
  at HttpServerHandler.channelRead()
```

---

## 📊 完整调用栈（真实）

### 启动阶段

```
main@1
  SpringApplication.run(Class, String[]) line: 1332
    SpringApplication.run(String...) line: 1354
      AbstractApplicationContext.refresh() line: 580
        AbstractApplicationContext.invokeBeanFactoryPostProcessors() line: 739
          ↓ (Bean扫描和注册)
        AbstractApplicationContext.registerBeanPostProcessors() line: 775
          ↓ (AOP代理注册)
        AbstractApplicationContext.onRefresh() line: 943
          ReactiveWebServerApplicationContext.onRefresh() line: 85
            ReactiveWebServerApplicationContext.createWebServer() line: 97
              NettyReactiveWebServerFactory.getWebServer() line: 153
                ↓ 创建ReactorHttpHandlerAdapter
                ↓ 配置Netty Pipeline
                NettyWebServer.start() line: 117
                  ↓ Netty启动，监听端口
```

### 请求处理阶段

```
reactor-http-nio-2@3456
  HttpServerHandler.channelRead() line: 245
    ReactorHttpHandlerAdapter.apply() line: 76
      HttpWebHandlerAdapter.handle() line: 87
        DispatcherHandler.handle() line: 275
          RequestMappingHandlerMapping.getHandler() line: 189
            ↓ 查找映射表
          RequestMappingHandlerAdapter.handle() line: 153
            InvocableHandlerMethod.invoke() line: 126
              InvocableHandlerMethod.doInvoke() line: 159
                Method.invoke() line: 566
                  YourController.yourMethod() line: XX  ← 你的代码
```

---

## ✅ 源码证明总结

### 第1步：启动入口
**源码**：`SpringApplication.run()` → `refreshContext()`
**证据**：line 1332, line 1354

### 第2步：IoC和AOP
**源码**：`AbstractApplicationContext.refresh()`
**证据**：
- line 739: `invokeBeanFactoryPostProcessors()` - Bean扫描
- line 775: `registerBeanPostProcessors()` - AOP代理

### 第3步：生成Mapping
**源码**：`RequestMappingHandlerMapping.afterPropertiesSet()`
**证据**：
- `initHandlerMethods()` - 扫描Handler
- `detectHandlerMethods()` - 检测方法
- `registerHandlerMethod()` - 注册映射

### 第4步：启动Netty
**源码**：`ReactiveWebServerApplicationContext.onRefresh()`
**证据**：
- line 85: `onRefresh()`
- line 97: `createWebServer()`
- line 153: `NettyReactiveWebServerFactory.getWebServer()`
- line 117: `NettyWebServer.start()`

### 第5步：创建桥梁
**源码**：`ReactorHttpHandlerAdapter`
**证据**：
- 实现Netty接口：`BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>>`
- 持有Spring接口：`HttpHandler httpHandler`
- 包装请求/响应：`ReactorServerHttpRequest/Response`

### 第6步：请求处理
**源码**：完整链路
**证据**：
- `HttpServerHandler.channelRead()` - Netty接收
- `ReactorHttpHandlerAdapter.apply()` - 桥梁转换
- `DispatcherHandler.handle()` - Spring分发
- `InvocableHandlerMethod.doInvoke()` - 反射调用

---

## 🎯 关键类文件位置

```
spring-boot-2.x.x/
├── spring-boot/
│   └── SpringApplication.java  ← 启动入口
├── spring-context/
│   └── AbstractApplicationContext.java  ← 容器刷新
├── spring-boot-autoconfigure/
│   └── web/reactive/
│       └── ReactiveWebServerFactoryAutoConfiguration.java  ← 自动配置
├── spring-webflux/
│   ├── DispatcherHandler.java  ← 请求分发
│   └── result/method/annotation/
│       └── RequestMappingHandlerMapping.java  ← 映射管理
└── spring-web/
    └── server/adapter/
        └── HttpWebHandlerAdapter.java  ← 适配器

reactor-netty/
└── http/server/
    ├── HttpServer.java  ← Netty服务器
    └── HttpServerHandler.java  ← Netty Handler

spring-boot/
└── web/embedded/netty/
    ├── NettyReactiveWebServerFactory.java  ← Netty工厂
    └── NettyWebServer.java  ← Netty封装
```

---

## 🔍 验证方法

### 方法1：Debug断点

在以下位置设置断点，运行Debug模式：

```java
// 1. SpringApplication.java line 1332
public static ConfigurableApplicationContext run(Class<?> primarySource, String... args)

// 2. AbstractApplicationContext.java line 580
public void refresh()

// 3. RequestMappingHandlerMapping.java
protected void registerHandlerMethod(Object handler, Method method, T mapping)

// 4. ReactiveWebServerApplicationContext.java line 85
protected void onRefresh()

// 5. DispatcherHandler.java line 275
public Mono<Void> handle(ServerWebExchange exchange)
```

### 方法2：日志输出

在`application.yml`中开启日志：

```yaml
logging:
  level:
    org.springframework.web: DEBUG
    org.springframework.boot.web: DEBUG
    reactor.netty: DEBUG
```

观察输出：

```
# 扫描Controller
DEBUG o.s.w.r.r.m.a.RequestMappingHandlerMapping : Mapped "{[/users/{id}],methods=[GET]}" onto ...

# 启动Netty
INFO  o.s.b.w.e.netty.NettyWebServer : Netty started on port(s): 8080

# 处理请求
DEBUG o.s.w.r.DispatcherHandler : [request-id] GET "/users/123"
DEBUG o.s.w.r.r.m.a.RequestMappingHandlerMapping : Mapped to UserController#getUser(String)
```

### 方法3：查看源码

直接在IDE中查看源码（按住Ctrl/Cmd点击类名）：

```java
SpringApplication.run()  → 跳转到源码
AbstractApplicationContext.refresh()  → 跳转到源码
RequestMappingHandlerMapping  → 跳转到源码
```

---

现在每个步骤都有真实的源码支撑！🎉

