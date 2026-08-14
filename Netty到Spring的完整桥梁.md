# Netty 到 Spring 的完整桥梁

## 🎯 核心问题

**Spring使用了哪个Handler来处理Netty的请求，让其进入到Spring容器？**

**答案：`ReactorHttpHandlerAdapter`**

---

## 📊 完整的桥梁流程

```
Netty层
    ↓
HttpServerOperations (Netty的Request/Response对象)
    ↓
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    【关键桥梁：ReactorHttpHandlerAdapter】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ↓
HttpWebHandlerAdapter (Spring的HttpHandler实现)
    ↓
DispatcherHandler (Spring容器的请求分发器)
    ↓
你的Controller
```

---

## 🔧 第1步：Spring创建桥梁Handler

### 源码：NettyReactiveWebServerFactory.getWebServer()

```java
// org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory

@Override
public WebServer getWebServer(HttpHandler httpHandler) {
    // httpHandler 是Spring容器的HttpHandler（包含DispatcherHandler）
    
    // 1. 创建Netty的HttpServer
    HttpServer httpServer = createHttpServer();
    
    // 2. 创建桥梁：ReactorHttpHandlerAdapter
    //    这是连接Netty和Spring的关键！
    ReactorHttpHandlerAdapter handlerAdapter = new ReactorHttpHandlerAdapter(httpHandler);
    //                                        ↑
    //                        将Spring的HttpHandler包装成Netty能调用的形式
    
    // 3. 将桥梁注入到Netty
    HttpServer configuredServer = httpServer.handle(handlerAdapter);
    //                                       ↑
    //                        Netty会调用这个handler处理所有请求
    
    // 4. 返回NettyWebServer
    return new NettyWebServer(configuredServer, getLifecycleTimeout());
}
```

**关键点**：
- ✅ Spring创建了 `ReactorHttpHandlerAdapter`
- ✅ 将Spring的 `HttpHandler` 传给它
- ✅ 通过 `httpServer.handle()` 注入到Netty

---

## 🔧 第2步：桥梁Handler的定义

### 源码：ReactorHttpHandlerAdapter

```java
// org.springframework.http.server.reactive.ReactorHttpHandlerAdapter

public class ReactorHttpHandlerAdapter 
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
    //         ↑
    //    实现了Netty的函数接口，Netty能直接调用
    
    private final HttpHandler httpHandler;  // Spring容器的HttpHandler
    
    public ReactorHttpHandlerAdapter(HttpHandler httpHandler) {
        Assert.notNull(httpHandler, "HttpHandler must not be null");
        this.httpHandler = httpHandler;  // ← 持有Spring的Handler
    }
    
    @Override
    public Publisher<Void> apply(HttpServerRequest request, HttpServerResponse response) {
        //                    ↑                ↑
        //         这两个是Netty的request和response (HttpServerOperations)
        
        // 1. 包装Netty的request为Spring的request
        ServerHttpRequest springRequest = new ReactorServerHttpRequest(
            request, this.bufferFactory
        );
        
        // 2. 包装Netty的response为Spring的response
        ServerHttpResponse springResponse = new ReactorServerHttpResponse(
            response, this.bufferFactory
        );
        
        // 3. 调用Spring容器的HttpHandler处理请求
        return this.httpHandler.handle(springRequest, springResponse);
        //     ↑
        //   调用Spring容器！请求进入Spring了！
    }
}
```

**关键点**：
- ✅ 实现了 `BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>>`
- ✅ 这是Netty能识别和调用的接口
- ✅ 内部持有Spring的 `HttpHandler`
- ✅ 将Netty对象包装成Spring对象
- ✅ 调用Spring的 `httpHandler.handle()` - **请求进入Spring容器！**

---

## 🔧 第3步：Netty如何调用这个Handler

### 源码：HttpServer.handle()

```java
// reactor.netty.http.server.HttpServer

public final HttpServer handle(
    BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler) {
    //  ↑
    // ReactorHttpHandlerAdapter实现了这个接口
    
    Objects.requireNonNull(handler, "handler");
    
    // 返回一个新的HttpServer，持有这个handler
    return new HttpServerHandle(this, handler);
}

// HttpServerHandle内部类
static final class HttpServerHandle extends HttpServer {
    final BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler;
    
    HttpServerHandle(HttpServer parent, BiFunction<...> handler) {
        super(parent);
        this.handler = Objects.requireNonNull(handler, "handler");
        //              ↑
        //        保存了ReactorHttpHandlerAdapter
    }
}
```

**关键点**：
- ✅ Netty的 `HttpServer` 持有了 `ReactorHttpHandlerAdapter`
- ✅ 当请求到达时，会调用这个handler

---

## 🔧 第4步：请求到达时的调用

### 简化的调用流程

```java
// 当HTTP请求到达时，Netty内部的流程（简化）

// 1. Netty EventLoop接收到请求
void onHttpRequest(HttpRequest nettyHttpRequest) {
    
    // 2. 创建HttpServerOperations（既是Request又是Response）
    HttpServerOperations ops = HttpServerOperations.create(
        connection, 
        nettyHttpRequest,
        ...
    );
    
    // 3. 获取之前注入的handler（ReactorHttpHandlerAdapter）
    BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> handler = 
        this.configuration.handler();  // ← 就是ReactorHttpHandlerAdapter
    
    // 4. 调用handler处理请求
    Publisher<Void> responsePublisher = handler.apply(ops, ops);
    //                                         ↑
    //            调用ReactorHttpHandlerAdapter.apply()
    //            请求从Netty进入Spring！
    
    // 5. 订阅响应
    Mono.from(responsePublisher).subscribe(
        v -> {},
        error -> handleError(error),
        () -> completeResponse()
    );
}
```

**关键点**：
- ✅ Netty调用 `handler.apply(ops, ops)`
- ✅ `handler` 就是 `ReactorHttpHandlerAdapter`
- ✅ 请求从这里进入Spring容器

---

## 🎨 完整的数据流向图

```
┌─────────────────────────────────────────────────────────────┐
│                    客户端发送请求                              │
│                 GET /users/123                               │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                     Netty 层                                 │
├─────────────────────────────────────────────────────────────┤
│  NioEventLoop 接收TCP连接                                    │
│    ↓                                                         │
│  HttpServerCodec 解码HTTP                                    │
│    ↓                                                         │
│  创建 HttpServerOperations                                   │
│    - 实现了 HttpServerRequest                                │
│    - 实现了 HttpServerResponse                               │
│    ↓                                                         │
│  调用 handler.apply(ops, ops)  ← 调用下面的桥梁              │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│         【桥梁】ReactorHttpHandlerAdapter                     │
│         (Spring 提供给 Netty 的 Handler)                     │
├─────────────────────────────────────────────────────────────┤
│  public Publisher<Void> apply(                               │
│      HttpServerRequest request,    ← Netty的对象            │
│      HttpServerResponse response   ← Netty的对象            │
│  ) {                                                         │
│      // 1. 包装为Spring对象                                  │
│      ServerHttpRequest springReq =                           │
│          new ReactorServerHttpRequest(request);              │
│                                                              │
│      ServerHttpResponse springResp =                         │
│          new ReactorServerHttpResponse(response);            │
│                                                              │
│      // 2. 调用Spring的HttpHandler                           │
│      return httpHandler.handle(springReq, springResp);       │
│      //     ↑                                                │
│      //  进入Spring容器！                                     │
│  }                                                           │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│              Spring 容器层                                    │
├─────────────────────────────────────────────────────────────┤
│  HttpWebHandlerAdapter                                       │
│    ↓                                                         │
│  创建 ServerWebExchange                                      │
│    ↓                                                         │
│  DispatcherHandler.handle(exchange)                          │
│    ↓                                                         │
│  查找 HandlerMapping                                         │
│    ↓                                                         │
│  找到: UserController.getUser()                              │
│    ↓                                                         │
│  执行业务逻辑                                                 │
│    ↓                                                         │
│  返回 Mono<User>                                             │
└─────────────────────────────────────────────────────────────┘
                        ↓
                   (响应返回...)
```

---

## 💻 关键代码片段汇总

### 1. Spring注入Handler到Netty

```java
// Spring侧：NettyReactiveWebServerFactory.getWebServer()

HttpServer httpServer = createHttpServer();

// 创建桥梁
ReactorHttpHandlerAdapter handlerAdapter = 
    new ReactorHttpHandlerAdapter(httpHandler);
    //                             ↑
    //                    Spring容器的HttpHandler

// 注入到Netty
httpServer = httpServer.handle(handlerAdapter);
//                       ↑
//              Netty会用这个handler处理所有请求
```

### 2. 桥梁Handler的实现

```java
// ReactorHttpHandlerAdapter

public class ReactorHttpHandlerAdapter 
    implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>> {
    
    private final HttpHandler httpHandler;  // Spring的
    
    @Override
    public Publisher<Void> apply(HttpServerRequest nettyReq, 
                                  HttpServerResponse nettyResp) {
        // 包装Netty对象
        ServerHttpRequest springReq = new ReactorServerHttpRequest(nettyReq);
        ServerHttpResponse springResp = new ReactorServerHttpResponse(nettyResp);
        
        // 进入Spring容器
        return this.httpHandler.handle(springReq, springResp);
    }
}
```

### 3. Netty调用Handler

```java
// Netty侧（简化）

HttpServerOperations ops = new HttpServerOperations(...);

// handler 就是 ReactorHttpHandlerAdapter
BiFunction<...> handler = this.configuration.handler();

// 调用handler，请求进入Spring
Publisher<Void> result = handler.apply(ops, ops);
```

---

## 🔍 如何验证？

### 方法1：Debug断点

在 `ReactorHttpHandlerAdapter.apply()` 设置断点：

```java
@Override
public Publisher<Void> apply(HttpServerRequest request, 
                              HttpServerResponse response) {
    // ← 断点设在这里
    
    System.out.println("=== Netty请求进入Spring ===");
    System.out.println("Netty Request类型: " + request.getClass().getName());
    // 输出: reactor.netty.http.server.HttpServerOperations
    
    System.out.println("调用栈: ");
    new Exception().printStackTrace();
    // 可以看到从Netty一路调用过来的栈
    
    // 包装并调用Spring
    ServerHttpRequest springRequest = new ReactorServerHttpRequest(request, ...);
    ServerHttpResponse springResponse = new ReactorServerHttpResponse(response, ...);
    
    return this.httpHandler.handle(springRequest, springResponse);
    // ← 单步进入这里，就进入Spring容器了
}
```

**发送请求**：
```bash
curl http://localhost:8080/any-endpoint
```

**观察调用栈**：
```
Thread: reactor-http-nio-2
  at ReactorHttpHandlerAdapter.apply() line: 76  ← 断点在这
  at reactor.netty.http.server.HttpServerOperations.onInboundNext()
  at reactor.netty.channel.ChannelOperationsHandler.channelRead()
  at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead()
  ...
```

### 方法2：添加日志

修改 `application.yml`：

```yaml
logging:
  level:
    org.springframework.http.server.reactive: DEBUG
    reactor.netty.http.server: DEBUG
```

**启动应用，发送请求**：

```
DEBUG reactor.netty.http.server.HttpServer : [id:0x...] Handler is being applied: {ReactorHttpHandlerAdapter}
DEBUG o.s.h.s.r.ReactorHttpHandlerAdapter : Applying handler adapter
DEBUG o.s.h.s.r.HttpWebHandlerAdapter : HTTP GET "/users/123"
```

### 方法3：自定义拦截器

```java
@Component
public class RequestInterceptor implements WebFilter {
    
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        System.out.println("=== 请求已进入Spring容器 ===");
        System.out.println("URI: " + exchange.getRequest().getURI());
        System.out.println("Method: " + exchange.getRequest().getMethod());
        System.out.println("Thread: " + Thread.currentThread().getName());
        
        return chain.filter(exchange);
    }
}
```

---

## ✅ 总结

### Spring使用的Handler

**`ReactorHttpHandlerAdapter`** - 这就是你要找的答案！

### 它的作用

1. ✅ **实现Netty的接口**
   ```java
   implements BiFunction<HttpServerRequest, HttpServerResponse, Publisher<Void>>
   ```

2. ✅ **持有Spring的Handler**
   ```java
   private final HttpHandler httpHandler;
   ```

3. ✅ **包装Netty对象为Spring对象**
   ```java
   ReactorServerHttpRequest
   ReactorServerHttpResponse
   ```

4. ✅ **调用Spring容器**
   ```java
   return httpHandler.handle(springRequest, springResponse);
   ```

### 完整链路

```
Netty创建HttpServerOperations
    ↓
调用 ReactorHttpHandlerAdapter.apply()  ← Spring的Handler
    ↓
包装为Spring对象
    ↓
调用 HttpWebHandlerAdapter.handle()  ← 进入Spring容器
    ↓
调用 DispatcherHandler.handle()  ← Spring的请求分发
    ↓
你的Controller
```

---

## 🎯 关键记忆点

```
问：Spring用什么Handler处理Netty请求？
答：ReactorHttpHandlerAdapter

问：它在哪里被创建？
答：NettyReactiveWebServerFactory.getWebServer()

问：它在哪里被注入到Netty？
答：httpServer.handle(handlerAdapter)

问：Netty如何调用它？
答：handler.apply(ops, ops)

问：它如何进入Spring容器？
答：httpHandler.handle(springRequest, springResponse)
```

现在Netty到Spring的桥梁清楚了吗？🎉

