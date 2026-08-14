# Spring WebFlux 完整源码链路分析

## 🎯 目标

基于 **Spring Boot、Spring WebFlux、Reactor Netty 的真实源码**，系统分析从应用启动到请求处理的完整链路。

---

## 📊 完整链路概览

```
SpringApplication.run()
    ↓
AbstractApplicationContext.refresh()
    ↓
【阶段1】IoC容器初始化
    ↓
【阶段2】扫描Controller，生成RequestMapping
    ↓
【阶段3】创建DispatcherHandler
    ↓
【阶段4】创建HttpHandler
    ↓
【阶段5】启动Netty服务器
    ↓
【阶段6】配置ReactorHttpHandlerAdapter
    ↓
【阶段7】请求处理链路
```

---

## 阶段1：Spring容器启动和刷新

### 1.1 启动入口

**源码位置**：`org.springframework.boot.SpringApplication`

```java
// SpringApplication.java

public static ConfigurableApplicationContext run(Class<?> primarySource, String... args) {
    return run(new Class<?>[] { primarySource }, args);
}

public ConfigurableApplicationContext run(String... args) {
    // 创建ApplicationContext
    context = createApplicationContext();
    
    // 准备context
    prepareContext(context, environment, listeners, applicationArguments, printedBanner);
    
    // 刷新context（关键！）
    refreshContext(context);
    
    return context;
}
```

### 1.2 容器刷新

**源码位置**：`org.springframework.context.support.AbstractApplicationContext`

```java
// AbstractApplicationContext.java

@Override
public void refresh() throws BeansException, IllegalStateException {
    synchronized (this.startupShutdownMonitor) {
        prepareRefresh();
        
        ConfigurableListableBeanFactory beanFactory = obtainFreshBeanFactory();
        prepareBeanFactory(beanFactory);
        
        try {
            postProcessBeanFactory(beanFactory);
            
            // 调用BeanFactoryPostProcessors（扫描配置类）
            invokeBeanFactoryPostProcessors(beanFactory);
            
            // 注册BeanPostProcessors（AOP代理）
            registerBeanPostProcessors(beanFactory);
            
            initMessageSource();
            initApplicationEventMulticaster();
            
            // 启动Web服务器（Netty在这里启动）
            onRefresh();
            
            registerListeners();
            
            // 实例化所有单例Bean（Controller在这里创建）
            finishBeanFactoryInitialization(beanFactory);
            
            finishRefresh();
        }
        catch (BeansException ex) {
            destroyBeans();
            throw ex;
        }
    }
}
```

---

## 阶段2：扫描Controller并生成Mapping

### 2.1 RequestMappingHandlerMapping初始化

**源码位置**：`org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping`

```java
// RequestMappingHandlerMapping.java

@Override
public void afterPropertiesSet() {
    this.config = new RequestMappingInfo.BuilderConfiguration();
    // 配置初始化...
    
    // 调用父类的afterPropertiesSet
    super.afterPropertiesSet();
}
```

### 2.2 扫描Handler方法

**源码位置**：`org.springframework.web.reactive.result.method.AbstractHandlerMethodMapping`

```java
// AbstractHandlerMethodMapping.java

@Override
public void afterPropertiesSet() {
    initHandlerMethods();
}

protected void initHandlerMethods() {
    // 获取所有Bean
    for (String beanName : getCandidateBeanNames()) {
        if (!beanName.startsWith(SCOPED_TARGET_NAME_PREFIX)) {
            processCandidateBean(beanName);
        }
    }
    handlerMethodsInitialized(getHandlerMethods());
}

protected void processCandidateBean(String beanName) {
    Class<?> beanType = null;
    try {
        beanType = obtainApplicationContext().getType(beanName);
    }
    catch (Throwable ex) {
        // ...
    }
    // 判断是否是Handler
    if (beanType != null && isHandler(beanType)) {
        detectHandlerMethods(beanName);
    }
}
```

### 2.3 判断是否是Handler

**源码位置**：`org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping`

```java
// RequestMappingHandlerMapping.java

@Override
protected boolean isHandler(Class<?> beanType) {
    return (AnnotatedElementUtils.hasAnnotation(beanType, Controller.class) ||
            AnnotatedElementUtils.hasAnnotation(beanType, RequestMapping.class));
}
```

### 2.4 检测Handler方法

**源码位置**：`org.springframework.web.reactive.result.method.AbstractHandlerMethodMapping`

```java
// AbstractHandlerMethodMapping.java

protected void detectHandlerMethods(Object handler) {
    Class<?> handlerType = (handler instanceof String ?
            obtainApplicationContext().getType((String) handler) : handler.getClass());
    
    if (handlerType != null) {
        Class<?> userType = ClassUtils.getUserClass(handlerType);
        Map<Method, T> methods = MethodIntrospector.selectMethods(userType,
                (MethodIntrospector.MetadataLookup<T>) method -> {
                    try {
                        return getMappingForMethod(method, userType);
                    }
                    catch (Throwable ex) {
                        throw new IllegalStateException("Invalid mapping on handler class [" +
                                userType.getName() + "]: " + method, ex);
                    }
                });
        
        methods.forEach((method, mapping) -> {
            Method invocableMethod = AopUtils.selectInvocableMethod(method, userType);
            registerHandlerMethod(handler, invocableMethod, mapping);
        });
    }
}
```

### 2.5 注册Handler方法

**源码位置**：`org.springframework.web.reactive.result.method.AbstractHandlerMethodMapping`

```java
// AbstractHandlerMethodMapping.java

protected void registerHandlerMethod(Object handler, Method method, T mapping) {
    this.mappingRegistry.register(mapping, handler, method);
}

// MappingRegistry内部类
class MappingRegistry {
    public void register(T mapping, Object handler, Method method) {
        this.readWriteLock.writeLock().lock();
        try {
            HandlerMethod handlerMethod = createHandlerMethod(handler, method);
            validateMethodMapping(handlerMethod, mapping);
            
            // 存储映射关系
            this.mappingLookup.put(mapping, handlerMethod);
            
            // 存储URL映射
            List<String> directUrls = getDirectUrls(mapping);
            for (String url : directUrls) {
                this.urlLookup.add(url, mapping);
            }
            
            // 打印日志
            if (logger.isTraceEnabled()) {
                logger.trace("Mapped \"" + mapping + "\" onto " + handlerMethod);
            }
            else if (logger.isInfoEnabled()) {
                logger.info("Mapped \"" + mapping + "\" onto " + handlerMethod);
            }
        }
        finally {
            this.readWriteLock.writeLock().unlock();
        }
    }
}
```

---

## 阶段3：创建DispatcherHandler

### 3.1 DispatcherHandler的自动配置

**源码位置**：`org.springframework.boot.autoconfigure.web.reactive.WebFluxAutoConfiguration`

```java
// WebFluxAutoConfiguration.java

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnClass(WebFluxConfigurer.class)
@ConditionalOnMissingBean({ WebFluxConfigurationSupport.class })
@AutoConfigureAfter({ ReactiveWebServerFactoryAutoConfiguration.class })
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE + 10)
public class WebFluxAutoConfiguration {
    
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties({ WebProperties.class })
    public static class WebFluxConfig implements WebFluxConfigurer {
        // 配置...
    }
}
```

### 3.2 DispatcherHandler Bean定义

**源码位置**：`org.springframework.web.reactive.config.DelegatingWebFluxConfiguration`

```java
// DelegatingWebFluxConfiguration.java

@Configuration(proxyBeanMethods = false)
public class DelegatingWebFluxConfiguration extends WebFluxConfigurationSupport {
    
    // DispatcherHandler会被自动创建
}
```

**源码位置**：`org.springframework.web.reactive.config.WebFluxConfigurationSupport`

```java
// WebFluxConfigurationSupport.java

@Bean
public DispatcherHandler webHandler() {
    return new DispatcherHandler();
}
```

---

## 阶段4：创建HttpHandler

### 4.1 HttpHandler的创建

**源码位置**：`org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext`

```java
// ReactiveWebServerApplicationContext.java

protected HttpHandler getHttpHandler() {
    // 从容器获取WebHandler（DispatcherHandler）
    String[] beanNames = getBeanFactory().getBeanNamesForType(WebHandler.class);
    if (beanNames.length == 0) {
        throw new ApplicationContextException(
                "Unable to start ReactiveWebApplicationContext due to missing WebHandler bean.");
    }
    if (beanNames.length > 1) {
        throw new ApplicationContextException(
                "Unable to start ReactiveWebApplicationContext due to multiple WebHandler beans : " +
                        StringUtils.arrayToCommaDelimitedString(beanNames));
    }
    
    // 获取DispatcherHandler
    WebHandler webHandler = getBeanFactory().getBean(beanNames[0], WebHandler.class);
    
    // 包装为HttpWebHandlerAdapter
    return WebHttpHandlerBuilder.applicationContext(this).build();
}
```

### 4.2 WebHttpHandlerBuilder构建

**源码位置**：`org.springframework.web.server.adapter.WebHttpHandlerBuilder`

```java
// WebHttpHandlerBuilder.java

public static WebHttpHandlerBuilder applicationContext(ApplicationContext context) {
    WebHttpHandlerBuilder builder = new WebHttpHandlerBuilder(
            context.getBean(WEB_HANDLER_BEAN_NAME, WebHandler.class), context);
    
    // 添加过滤器
    List<WebFilter> webFilters = context
            .getBeanProvider(WebFilter.class)
            .orderedStream()
            .collect(Collectors.toList());
    builder.filters(filters -> filters.addAll(webFilters));
    
    // 添加异常处理器
    List<WebExceptionHandler> exceptionHandlers = context
            .getBeanProvider(WebExceptionHandler.class)
            .orderedStream()
            .collect(Collectors.toList());
    builder.exceptionHandlers(handlers -> handlers.addAll(exceptionHandlers));
    
    return builder;
}

public HttpHandler build() {
    // 创建HttpWebHandlerAdapter
    WebHandler decorated = new FilteringWebHandler(this.webHandler, this.filters);
    decorated = new ExceptionHandlingWebHandler(decorated,  this.exceptionHandlers);
    
    HttpWebHandlerAdapter adapted = new HttpWebHandlerAdapter(decorated);
    if (this.sessionManager != null) {
        adapted.setSessionManager(this.sessionManager);
    }
    // 其他配置...
    
    return adapted;
}
```

---

## 阶段5：启动Netty服务器

### 5.1 onRefresh触发启动

**源码位置**：`org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext`

```java
// ReactiveWebServerApplicationContext.java

@Override
protected void onRefresh() {
    super.onRefresh();
    try {
        createWebServer();
    }
    catch (Throwable ex) {
        throw new ApplicationContextException("Unable to start reactive web server", ex);
    }
}

private void createWebServer() {
    ServerManager serverManager = this.serverManager;
    if (serverManager == null) {
        StartupStep createWebServer = this.getApplicationStartup()
                .start("spring.boot.webserver.create");
        
        // 获取HttpHandler
        HttpHandler httpHandler = getHttpHandler();
        
        // 获取WebServerFactory
        ReactiveWebServerFactory factory = getWebServerFactory();
        
        createWebServer.tag("factory", factory.getClass().toString());
        
        // 创建WebServer（启动Netty）
        this.webServer = factory.getWebServer(httpHandler);
        
        createWebServer.end();
        // ...
    }
    initPropertySources();
}
```

### 5.2 NettyReactiveWebServerFactory创建WebServer

**源码位置**：`org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory`

```java
// NettyReactiveWebServerFactory.java

@Override
public WebServer getWebServer(HttpHandler httpHandler) {
    // 1. 创建Reactor Netty的HttpServer
    HttpServer httpServer = createHttpServer();
    
    // 2. 创建ReactorHttpHandlerAdapter（桥梁）
    ReactorHttpHandlerAdapter handlerAdapter = new ReactorHttpHandlerAdapter(httpHandler);
    
    // 3. 配置handler到HttpServer
    HttpServer serverWithHandler = httpServer.handle(handlerAdapter);
    
    // 4. 返回NettyWebServer
    return new NettyWebServer(
            serverWithHandler, 
            getLifecycleTimeout(), 
            getShutdown());
}

private HttpServer createHttpServer() {
    HttpServer server = HttpServer.create();
    // 应用自定义配置...
    return server;
}
```

### 5.3 HttpServer.handle()

**源码位置**：`reactor.netty.http.server.HttpServer`

```java
// HttpServer.java (Reactor Netty)

public final HttpServer handle(
        BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler) {
    Objects.requireNonNull(handler, "handler");
    return new HttpServerHandle(handler);
}

// 内部类
static final class HttpServerHandle extends HttpServer {
    final BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler;
    
    HttpServerHandle(BiFunction<...> handler) {
        this.handler = handler;
    }
    
    @Override
    protected HttpServer duplicate() {
        return new HttpServerHandle(this.handler);
    }
    
    @Override
    protected ConnectionObserver createObserver() {
        return new HttpServerHandle.Handle(this.handler);
    }
}
```

**注意**：`HttpServerHandle`实际上会创建一个`Handle`内部类作为`ConnectionObserver`。

### 5.4 NettyWebServer启动

**源码位置**：`org.springframework.boot.web.embedded.netty.NettyWebServer`

```java
// NettyWebServer.java

@Override
public void start() throws WebServerException {
    if (this.disposableServer == null) {
        try {
            this.disposableServer = startHttpServer();
        }
        catch (Exception ex) {
            throw new WebServerException("Unable to start Netty", ex);
        }
        // 日志
        if (logger.isInfoEnabled()) {
            logger.info("Netty started on port(s): " + getPort());
        }
        startDaemonAwaitThread(this.disposableServer);
    }
}

private DisposableServer startHttpServer() {
    // 绑定端口并启动
    return this.httpServer.bindNow();
}
```

---

## 阶段6：Netty请求处理链路

### 6.1 Netty Pipeline配置

**源码位置**：`reactor.netty.http.server.HttpServerBind`

```java
// HttpServerBind.java (Reactor Netty)

// Pipeline会在连接建立时被配置
// 添加HTTP编解码器、聚合器等
```

### 6.2 ConnectionObserver监听状态

当HTTP请求到达并被解析后，连接状态变为`REQUEST_RECEIVED`，触发`ConnectionObserver.onStateChange()`。

**源码位置**：`reactor.netty.http.server.HttpServerOperations`

```java
// HttpServerOperations.java (Reactor Netty)

// 当请求被接收后，会通知所有ConnectionObserver
// 包括之前创建的HttpServerHandle
```

### 6.3 HttpServerHandle处理请求

根据用户提供的代码片段，`HttpServerHandle`实现了`ConnectionObserver`：

```java
// HttpServerHandle (ConnectionObserver)

@Override
public void onStateChange(Connection connection, State newState) {
    if (newState == HttpServerState.REQUEST_RECEIVED) {
        HttpServerOperations ops = (HttpServerOperations) connection;
        
        // 调用handler（ReactorHttpHandlerAdapter）
        Publisher<Void> publisher = handler.apply(ops, ops);
        
        // 包装并订阅
        Mono<Void> mono = Mono.deferContextual(ctx -> {
            ops.currentContext = Context.of(ctx);
            return Mono.fromDirect(publisher);
        });
        
        mono.subscribe(ops.disposeSubscriber());
    }
}
```

---

## 阶段7：进入Spring容器

### 7.1 ReactorHttpHandlerAdapter

**源码位置**：`org.springframework.http.server.reactive.ReactorHttpHandlerAdapter`

```java
// ReactorHttpHandlerAdapter.java

public class ReactorHttpHandlerAdapter 
        implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
    
    private final HttpHandler httpHandler;
    
    public ReactorHttpHandlerAdapter(HttpHandler httpHandler) {
        Assert.notNull(httpHandler, "HttpHandler must not be null");
        this.httpHandler = httpHandler;
    }
    
    @Override
    public Publisher<Void> apply(HttpServerRequest reactorRequest, 
                                   HttpServerResponse reactorResponse) {
        
        // 包装Netty对象为Spring对象
        ServerHttpRequest request = createRequest(reactorRequest, bufferFactory);
        ServerHttpResponse response = createResponse(reactorResponse, bufferFactory);
        
        // 调用Spring的HttpHandler
        return this.httpHandler.handle(request, response);
    }
}
```

### 7.2 HttpWebHandlerAdapter

**源码位置**：`org.springframework.http.server.reactive.HttpWebHandlerAdapter`

```java
// HttpWebHandlerAdapter.java

public class HttpWebHandlerAdapter extends WebHandlerDecorator implements HttpHandler {
    
    @Override
    public Mono<Void> handle(ServerHttpRequest request, ServerHttpResponse response) {
        // 创建ServerWebExchange
        ServerWebExchange exchange = createExchange(request, response);
        
        // 调用DispatcherHandler
        return getDelegate().handle(exchange)
                .doOnSuccess(aVoid -> logResponse(exchange))
                .onErrorResume(ex -> handleUnresolvedError(exchange, ex))
                .then(Mono.defer(response::setComplete));
    }
    
    protected ServerWebExchange createExchange(ServerHttpRequest request, 
                                                ServerHttpResponse response) {
        return new DefaultServerWebExchange(request, response, this.sessionManager,
                getCodecConfigurer(), getLocaleContextResolver(), this.applicationContext);
    }
}
```

### 7.3 DispatcherHandler

**源码位置**：`org.springframework.web.reactive.DispatcherHandler`

```java
// DispatcherHandler.java

public class DispatcherHandler implements WebHandler, ApplicationContextAware {
    
    @Nullable
    private List<HandlerMapping> handlerMappings;
    
    @Nullable
    private List<HandlerAdapter> handlerAdapters;
    
    @Nullable
    private List<HandlerResultHandler> resultHandlers;
    
    @Override
    public Mono<Void> handle(ServerWebExchange exchange) {
        if (this.handlerMappings == null) {
            return createNotFoundError();
        }
        
        // 查找Handler
        return Flux.fromIterable(this.handlerMappings)
                .concatMap(mapping -> mapping.getHandler(exchange))
                .next()
                .switchIfEmpty(createNotFoundError())
                // 调用Handler
                .flatMap(handler -> invokeHandler(exchange, handler))
                // 处理结果
                .flatMap(result -> handleResult(exchange, result));
    }
    
    private Mono<HandlerResult> invokeHandler(ServerWebExchange exchange, Object handler) {
        if (this.handlerAdapters != null) {
            for (HandlerAdapter handlerAdapter : this.handlerAdapters) {
                if (handlerAdapter.supports(handler)) {
                    return handlerAdapter.handle(exchange, handler);
                }
            }
        }
        return Mono.error(new IllegalStateException("No HandlerAdapter: " + handler));
    }
    
    private Mono<Void> handleResult(ServerWebExchange exchange, HandlerResult result) {
        return getResultHandler(result).handleResult(exchange, result)
                .checkpoint("Handler " + result.getHandler() + " [DispatcherHandler]")
                .onErrorResume(ex -> result.applyExceptionHandler(ex)
                        .flatMap(exResult -> {
                            String text = "Exception handler " + exResult.getHandler() +
                                    ", error=\"" + ex.getMessage() + "\" [DispatcherHandler]";
                            return getResultHandler(exResult).handleResult(exchange, exResult).checkpoint(text);
                        }));
    }
}
```

---

## 📊 完整调用栈

```
main线程：
  SpringApplication.run()
    AbstractApplicationContext.refresh()
      invokeBeanFactoryPostProcessors()  // 扫描配置
      registerBeanPostProcessors()  // AOP
      onRefresh()
        ReactiveWebServerApplicationContext.createWebServer()
          NettyReactiveWebServerFactory.getWebServer(httpHandler)
            new ReactorHttpHandlerAdapter(httpHandler)
            HttpServer.handle(handlerAdapter)
            new NettyWebServer(serverWithHandler)
          NettyWebServer.start()
            HttpServer.bindNow()  // Netty启动
      finishBeanFactoryInitialization()
        RequestMappingHandlerMapping.afterPropertiesSet()
          initHandlerMethods()
            detectHandlerMethods()
              registerHandlerMethod()  // 注册映射

reactor-http-nio线程（请求处理）：
  Netty EventLoop接收请求
    HttpServerCodec解码
    HttpServerOperations创建
    状态变为 REQUEST_RECEIVED
    HttpServerHandle.onStateChange()
      handler.apply(ops, ops)  // ReactorHttpHandlerAdapter
        ReactorHttpHandlerAdapter.apply()
          httpHandler.handle(request, response)
            HttpWebHandlerAdapter.handle()
              createExchange()
              DispatcherHandler.handle(exchange)
                mapping.getHandler(exchange)  // 查找Handler
                handlerAdapter.handle(exchange, handler)  // 调用Controller
                  YourController.yourMethod()
                resultHandler.handleResult()  // 处理返回值
```

---

## ✅ 总结

### 关键类和接口

| 层次 | 类/接口 | 作用 |
|------|---------|------|
| **启动** | `SpringApplication` | 应用启动入口 |
| **容器** | `AbstractApplicationContext` | Spring容器 |
| **配置** | `WebFluxAutoConfiguration` | 自动配置 |
| **映射** | `RequestMappingHandlerMapping` | URL映射管理 |
| **分发** | `DispatcherHandler` | 请求分发器 |
| **适配** | `HttpWebHandlerAdapter` | WebHandler适配器 |
| **桥梁** | `ReactorHttpHandlerAdapter` | Netty→Spring桥梁 |
| **工厂** | `NettyReactiveWebServerFactory` | 创建Netty服务器 |
| **服务器** | `NettyWebServer` | Netty服务器封装 |
| **观察者** | `HttpServerHandle` (ConnectionObserver) | 监听请求状态 |
| **请求** | `HttpServerOperations` | Netty的请求/响应 |

### 核心流程

1. ✅ Spring容器启动（`refresh()`）
2. ✅ 扫描Controller，生成映射表
3. ✅ 创建DispatcherHandler
4. ✅ 创建HttpHandler（包含DispatcherHandler）
5. ✅ 启动Netty（`onRefresh()`）
6. ✅ 创建ReactorHttpHandlerAdapter桥梁
7. ✅ HttpServerHandle监听请求状态
8. ✅ 请求到达，调用ReactorHttpHandlerAdapter
9. ✅ 进入Spring容器（DispatcherHandler）
10. ✅ 执行Controller，返回结果

---

这是基于Spring和Reactor Netty实际源码的完整分析！🎉

