package org.example.springwebflux.controller;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/11/23 14:51
 * @Description:
 */
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.buffer.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.FutureMono;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@RestController
public class HelloController {

    AtomicInteger num = new AtomicInteger(0);

    @Autowired
    WebClient webClient;

    @GetMapping("/hello/{name}")
    public Mono<String> hello(@PathVariable String name) {
        return Mono.just("Hello " + name);
    }

    @PostMapping("/live")
    public Flux<String> live(ServerHttpRequest httpRequest) {
        Flux<DataBuffer> body = httpRequest.getBody();
        Flux flux = Flux.just("ok");
        flux.subscribe(new Consumer() {
            @Override
            public void accept(Object o) {
                body.flatMap(new Function<DataBuffer, Publisher<?>>() {
                    @Override
                    public Publisher<?> apply(DataBuffer dataBuffer) {
                        InputStream inputStream = dataBuffer.asInputStream();
                        return null;
                    }
                });
                System.out.println(o);
            }
        });
        return flux;
    }

    @PostMapping("/live2")
    public Mono<String> live2(ServerWebExchange serverWebExchange) throws IOException {

        Flux<DataBuffer> requestBody = serverWebExchange.getRequest().getBody();
        Mono<String> body = getBodyAsBytes(requestBody);

//        body.subscribe(new Consumer<byte[]>() {
//            @Override
//            public void accept(byte[] bytes) {
//                ServerWebExchange modify = mutateServerWebExchange(serverWebExchange,serverWebExchange.getRequest(),bytes);
//                System.out.println(bytes.length);
//            }
//        });
        Mono mono =  Mono.defer(new Supplier<Mono<?>>() {
            @Override
            public Mono<?> get() {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return Mono.just("ok");
            }
        });
//        mono.subscribe(new Consumer<String>() {
//            @Override
//            public void accept(String s) {
//                System.out.println();
//            }
//        });
        mono = mono.doFinally(new Consumer<SignalType>() {
            @Override
            public void accept(SignalType signalType) {
                System.out.println();
            }
        });
         return mono;
    }

    private ServerWebExchange mutateServerWebExchange(
            ServerWebExchange exchange, ServerHttpRequest request, byte[] requestBodyInBytes) {
        ServerHttpRequest modifiedRequest =
                new ServerHttpRequestDecorator(request) {
                    @Override
                    public Flux<DataBuffer> getBody() {
                        return DataBufferUtils.read(new ByteArrayResource(requestBodyInBytes),
                                new NettyDataBufferFactory(ByteBufAllocator.DEFAULT),
                                requestBodyInBytes.length);
                    }
                };

        return exchange;
    }

    public Mono<String> getBodyAsBytes(Flux<DataBuffer> body) {
        return DataBufferUtils.join(body)
                .map(
                        dataBuffer -> {
                            byte[] bytes = new byte[dataBuffer.readableByteCount()];
                            dataBuffer.read(bytes);
                            DataBufferUtils.release(dataBuffer);
                            System.out.println(bytes.length);
                            return "ok";
                        }
                );
    }

    public Mono<Void> getBodyAsBytes2(ServerWebExchange serverWebExchange,Flux<DataBuffer> body) {
        Flux<ByteBuf> byteBufFlux = Flux.from(body).map(NettyDataBufferFactory::toByteBuf);


        Mono<Void> monoSource = new Mono(){
            @Override
            public void subscribe(CoreSubscriber actual) {

                Mono.<Void>create(new Consumer<MonoSink<Void>>() {
                    @Override
                    public void accept(MonoSink<Void> voidMonoSink) {

                        Subscriber subscriber = new Subscriber(){
                            @Override
                            public void onSubscribe(Subscription s) {
                                System.out.println();
                                s.request(128);
                            }

                            @Override
                            public void onNext(Object o) {
                                System.out.println(num.getAndIncrement());
                                System.out.println(((ByteBuf) o).duplicate().toString(Charset.defaultCharset()));
//                        Flux<DataBuffer> dataBufferFlux = createFluxFromStr("okk");
//                        serverWebExchange.getResponse().writeWith(dataBufferFlux);
                                if (num.get() > 100){
                                    voidMonoSink.success();
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                System.out.println();
                            }

                            @Override
                            public void onComplete() {
                                System.out.println();
                            }
                        };
                        byteBufFlux.subscribe(subscriber);

                    }
                }).subscribe(actual);
            }
        };

        Mono<Void> mono = FutureMono.deferFuture(new Supplier<Future<Void>>() {
            @Override
            public Future<Void> get() {
                return new Future<Void>() {
                    @Override
                    public boolean isSuccess() {
                        return true;
                    }

                    @Override
                    public boolean isCancellable() {
                        return false;
                    }

                    @Override
                    public Throwable cause() {
                        return null;
                    }

                    @Override
                    public Future<Void> addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
                        return null;
                    }

                    @Override
                    public Future<Void> addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
                        return null;
                    }

                    @Override
                    public Future<Void> removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
                        return null;
                    }

                    @Override
                    public Future<Void> removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
                        return null;
                    }

                    @Override
                    public Future<Void> sync() throws InterruptedException {
                        return null;
                    }

                    @Override
                    public Future<Void> syncUninterruptibly() {
                        return null;
                    }

                    @Override
                    public Future<Void> await() throws InterruptedException {
                        return null;
                    }

                    @Override
                    public Future<Void> awaitUninterruptibly() {
                        return null;
                    }

                    @Override
                    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
                        return false;
                    }

                    @Override
                    public boolean await(long timeoutMillis) throws InterruptedException {
                        return false;
                    }

                    @Override
                    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
                        return false;
                    }

                    @Override
                    public boolean awaitUninterruptibly(long timeoutMillis) {
                        return false;
                    }

                    @Override
                    public Void getNow() {
                        return null;
                    }

                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public Void get() throws InterruptedException, ExecutionException {
                        return null;
                    }

                    @Override
                    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                        return null;
                    }
                };
            }
        });


        Mono<Void> voidMono = mono.then();
        return voidMono.thenEmpty(monoSource);
    }

    public Mono<Void> getBodyAsBytes3(ServerWebExchange serverWebExchange,Flux<DataBuffer> body) {
        Flux<ByteBuf> byteBufFlux = Flux.from(body).map(NettyDataBufferFactory::toByteBuf);


        Mono<Void> monoSource = new Mono(){
            @Override
            public void subscribe(CoreSubscriber actual) {
                Subscriber subscriber = new Subscriber(){
                    @Override
                    public void onSubscribe(Subscription s) {
                        System.out.println();
                        s.request(128);
                    }

                    @Override
                    public void onNext(Object o) {
                        System.out.println(num.getAndSet(1));
                        System.out.println(((ByteBuf) o).duplicate().toString(Charset.defaultCharset()));
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println();
                    }

                    @Override
                    public void onComplete() {
                        System.out.println();
                    }
                };
                byteBufFlux.subscribe(subscriber);
            }
        };

        Mono<Void> mono = FutureMono.deferFuture(new Supplier<Future<Void>>() {
            @Override
            public Future<Void> get() {
                return new Future<Void>() {
                    @Override
                    public boolean isSuccess() {
                        return true;
                    }

                    @Override
                    public boolean isCancellable() {
                        return false;
                    }

                    @Override
                    public Throwable cause() {
                        return null;
                    }

                    @Override
                    public Future<Void> addListener(GenericFutureListener<? extends Future<? super Void>> listener) {
                        return null;
                    }

                    @Override
                    public Future<Void> addListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
                        return null;
                    }

                    @Override
                    public Future<Void> removeListener(GenericFutureListener<? extends Future<? super Void>> listener) {
                        return null;
                    }

                    @Override
                    public Future<Void> removeListeners(GenericFutureListener<? extends Future<? super Void>>... listeners) {
                        return null;
                    }

                    @Override
                    public Future<Void> sync() throws InterruptedException {
                        return null;
                    }

                    @Override
                    public Future<Void> syncUninterruptibly() {
                        return null;
                    }

                    @Override
                    public Future<Void> await() throws InterruptedException {
                        return null;
                    }

                    @Override
                    public Future<Void> awaitUninterruptibly() {
                        return null;
                    }

                    @Override
                    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
                        return false;
                    }

                    @Override
                    public boolean await(long timeoutMillis) throws InterruptedException {
                        return false;
                    }

                    @Override
                    public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
                        return false;
                    }

                    @Override
                    public boolean awaitUninterruptibly(long timeoutMillis) {
                        return false;
                    }

                    @Override
                    public Void getNow() {
                        return null;
                    }

                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public Void get() throws InterruptedException, ExecutionException {
                        return null;
                    }

                    @Override
                    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                        return null;
                    }
                };
            }
        });
        Mono<Void> voidMono = mono.then();
        voidMono.thenEmpty(monoSource);
        Mono.defer(() -> Mono.fromDirect(voidMono))
                .subscribe(new Consumer<Void>() {
                    @Override
                    public void accept(Void unused) {

                    }
                });
        return voidMono;
    }


    @PostMapping("/live3")
    public Mono<Void> live3(ServerWebExchange exchange) throws IOException {

        Mono<Void> mono = webClient
                .method(HttpMethod.POST)
                .uri("http://localhost:8082/api/log/upload")
                .headers(headers -> {
                    headers.addAll(exchange.getRequest().getHeaders());
                })
                .body(BodyInserters.fromDataBuffers(exchange.getRequest().getBody()))
                .exchange()
//                .timeout(Duration.ofMillis(2000))
                .flatMap(new MyFunction(exchange))
                .doOnError(throwable -> cleanup(exchange))
                .doFinally(signalType -> {

                });
        return mono;
    }

    private class MyFunction implements Function<ClientResponse, Mono<Void>>{
        ServerWebExchange exchange;
        public MyFunction(ServerWebExchange exchange){
            this.exchange = exchange;
        }

        @Override
        public Mono<Void> apply(ClientResponse clientResponse) {
            Flux<DataBuffer> body = clientResponse.body(BodyExtractors.toDataBuffers());
            MediaType contentType = null;
            try {
                contentType = clientResponse.headers().contentType().get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            exchange.getAttributes().put("test_response", clientResponse);
            return (false
                    ? exchange.getResponse().writeAndFlushWith(body.map(Flux::just))
                    : exchange.getResponse().writeWith(body))
                    .doOnCancel(() -> cleanup(exchange));
        }
    }

    public Flux<DataBuffer> createFluxFromStr(String str) {
        DefaultDataBufferFactory defaultDataBufferFactory = new DefaultDataBufferFactory();
        // 将字符串转换为字节数组
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        // 创建DataBuffer
        DataBuffer buffer = defaultDataBufferFactory.wrap(bytes);

        // 将DataBuffer封装成Flux
        return Flux.just(buffer)
                .doFinally(signalType -> DataBufferUtils.release(buffer)); // 确保释放DataBuffer资源
    }

    @PostMapping("/live4")
    public Mono<Void> live4(ServerWebExchange exchange) throws IOException {

        Mono<Void> mono = webClient
                .method(HttpMethod.POST)
                .uri("http://localhost:8082/hello")
                .headers(headers -> {
                    headers.addAll(exchange.getRequest().getHeaders());
                })
                .body(BodyInserters.fromDataBuffers(exchange.getRequest().getBody()))
                .exchange()
//                .timeout(Duration.ofMillis(2000))

                .flatMap(clientResponse -> {
                    Flux<DataBuffer> body = clientResponse.body(BodyExtractors.toDataBuffers());
                    MediaType contentType = null;
                    try {
                        contentType = clientResponse.headers().contentType().get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    exchange.getAttributes().put("test_response", clientResponse);
                    return (false
                            ? exchange.getResponse().writeAndFlushWith(body.map(Flux::just))
                            : exchange.getResponse().writeWith(body))
                            .doOnCancel(() -> cleanup(exchange));
                })
                .doOnError(throwable -> cleanup(exchange))
                .doFinally(signalType -> {

                });
        return mono;
    }

    @PostMapping("/live5")
    public Mono<Void> live5(ServerWebExchange serverWebExchange) throws IOException {
        Flux<DataBuffer> requestBody = serverWebExchange.getRequest().getBody();
        Mono<Void> body = getBodyAsBytes2(serverWebExchange,requestBody);
        return body;
    }

    private void cleanup(ServerWebExchange exchange) {
        ClientResponse clientResponse = exchange.getAttribute("test_response");
        if (clientResponse != null) {
            clientResponse.bodyToMono(Void.class).subscribe();
        }
    }


}
