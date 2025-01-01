package org.example.springwebflux.controller;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.example.springwebflux.model.Image;
import org.example.springwebflux.model.ImageResponse;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import reactor.netty.FutureMono;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/12/1 15:24
 * @Description:
 */
@RestController
public class FlushResponseController {

    AtomicInteger index = new AtomicInteger(1);
    boolean isImage = true;

    /**
     * 返回值
     * [
     *     1,
     *     2,
     *     3,
     *     4,
     *     5
     * ]
     * */
    @GetMapping("/flux/{name}")
    public Flux<Integer> hello(@PathVariable String name) {
        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5);
        flux.subscribe(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println(integer);
            }
        });
        flux.subscribe(new TestSub());
        return flux;
    }

    @GetMapping("/flux/link")
    public Flux<Integer> link() {
        Flux<Integer> flux = Flux.just(1);

//        Flux<Integer> flux1 = flux.flatMap(new Function<Integer, Publisher<? extends Integer>>() {
//            @Override
//            public Publisher<? extends Integer> apply(Integer integer) {
//                return Mono.just(integer);
//            }
//        });
        Flux<Integer> flux1 = flux.flatMap(new Function<Integer, Publisher<Integer>>() {
            @Override
            public Publisher<Integer> apply(Integer integer) {
                return new CorePublisher<Integer>() {

                    @Override
                    public void subscribe(Subscriber<? super Integer> s) {
                        CorePublisher publisher = Operators.onLastAssembly(this);
                        CoreSubscriber subscriber = Operators.toCoreSubscriber(s);
                        try {

                            publisher.subscribe(subscriber);
                        }
                        catch (Throwable e) {
                            Operators.reportThrowInSubscribe(subscriber, e);
                            return;
                        }
                    }

                    @Override
                    public void subscribe(CoreSubscriber<? super Integer> subscriber) {
                        subscriber.onSubscribe(new Subscription() {
                            @Override
                            public void request(long n) {
                                subscriber.onNext(integer);
                                subscriber.onComplete();
                            }

                            @Override
                            public void cancel() {

                            }
                        });
//                        subscriber.onSubscribe(Operators.scalarSubscription(subscriber, integer));
                    }
                };
            }
        });


//        flux1 = flux1.flatMap(new Function<Integer, Publisher<Integer>>() {
//            @Override
//            public Publisher<Integer> apply(Integer integer) {
//                return Mono.just(integer+10);
//            }
//        });

        Mono mono = new Mono() {
            @Override
            public void subscribe(CoreSubscriber actual) {
                System.out.println("mono 1被调用,actual="+actual);
            }
        };

        flux1.subscribe(new CoreSubscriberTest());
        flux1.subscribe(new CoreSubscriberTest());


        return flux1;
    }

    private class CoreSubscriberTest implements CoreSubscriber<Integer>{

        @Override
        public void onSubscribe(Subscription s) {
            System.out.println("Subscription 1");
        }

        @Override
        public void onNext(Integer integer) {
            System.out.println("onNext 1" + integer);
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onComplete() {

        }
    }

    private class SubscriberTest implements Subscription{

        @Override
        public void request(long n) {
            System.out.println("request:"+n);
        }

        @Override
        public void cancel() {

        }
    }

    @GetMapping("/defer/{name}")
    public Flux<String> defer(@PathVariable String name) {
        Flux<String> flux = Flux.defer(() -> {
            // 这里可以执行异步操作，例如从另一个线程获取数据
            try {
                Thread.sleep(1000);
                System.out.println("数据生成成功");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return Flux.just("data1", "data2");
        });
//        flux.subscribe(new TestSub());
        System.out.println("flux 返回");
        return flux;
    }

    @PostMapping("/upload")
    public Mono<String>  live4(ServerWebExchange exchange) throws IOException {
        Flux<DataBuffer> requestBody = exchange.getRequest().getBody();

        Flux<ByteBuf> byteBufFlux = requestBody.map(NettyDataBufferFactory::toByteBuf);
        CompletableFuture<String> future = new CompletableFuture<>();
        byteBufFlux.subscribe(new HttpBodySub(future));
        Mono<String> mono = Mono.fromFuture(future);

//        mono.subscribe(result -> System.out.println(result)); // 异步任务完成后
        mono.subscribe(new TestSub());
        System.out.println("处理器接收请求");
        return mono;
    }

    @GetMapping("/download")
    public Mono<ImageResponse> download(){
        return Mono.just(completeImage);
    }

    @GetMapping("/writeToDisk")
    public Mono<String> writeToDisk() throws IOException {
        File  outputFile = new File("/Users/linsong.chen/Downloads/image/outputImage_"+index.getAndAdd(1)+".jpg");
        FileOutputStream  os = new FileOutputStream(outputFile);
                    // 将字节流写入到文件中
        os.write(completeImage.getBody());
        // 确保所有数据都被写入
        os.flush();
        return Mono.just("成功");
    }

    public class HttpBodySub implements Subscriber<ByteBuf>{
        Subscription s;
        CompletableFuture future;

        public HttpBodySub(CompletableFuture future) {
            this.future = future;
        }

        @Override
        public void onSubscribe(Subscription s) {
            System.out.println("body subscription:"+s);
            s.request(1);
            this.s = s;
        }

        @Override
        public void onNext(ByteBuf byteBuf) {
            if (isImage){
                writeToFile(byteBuf);
                byteBuf.release();
            }else {
                System.out.println(new Date()+" body onNext:"+byteBuf.toString(Charset.defaultCharset()));
            }

            s.request(1);
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onComplete() {
            future.complete("异步任务完成");
            System.out.println("body onComplete");
        }
    }

    File outputFile;
    OutputStream os;
    long len = 0;
    ImageResponse completeImage;
    Image imageInProgress;
    long start = 0;
    long endTime = 0;
//    private synchronized void writeToFile1(ByteBuf byteBuf){
//        // 假设imageBytes是包含图片数据的字节数组
//        byte[] imageBytes = new byte[byteBuf.readableBytes()];
//        len += imageBytes.length;
//        byteBuf.readBytes(imageBytes);
//        byte[] lastThreeByte = Arrays.copyOfRange(imageBytes,imageBytes.length-3,imageBytes.length);
//        String end = new String(lastThreeByte);
//        if ("end".equals(end)){
//            System.out.println("找到end");
//        }
//        try {
//            if (outputFile == null){
//                System.out.println("创建新文件");
//                imageInProgress = new Image();
//                ByteBuf buf = Unpooled.buffer(1024);
//                imageInProgress.setBuf(buf);
//                // 创建一个文件输出流，用于写入图片数据
//                outputFile = new File("/Users/linsong.chen/Downloads/image/outputImage_"+index.getAndAdd(1)+".jpg");
//                os = new FileOutputStream(outputFile);
//
//            }
//            ByteBuf buf = imageInProgress.getBuf();
//            buf.writeBytes(imageBytes);
//            imageInProgress.setLen(len);
////            // 将字节流写入到文件中
////            os.write(imageBytes);
////            // 确保所有数据都被写入
////            os.flush();
//
//            if ("end".equals(end)){
//                completeImage = imageInProgress;
//                imageInProgress = null;
//                // 关闭输出流
//                os.close();
//                System.out.println("文件大小:"+len);
//                outputFile = null;
//                os = null;
//                len = 0;
//
//            }
//
//            System.out.println("写入成功！大小:"+imageBytes.length+" 文件序号:"+index.get());
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("图片写入失败！");
//        }
//    }

    private synchronized void writeToFile(ByteBuf byteBuf){
        // 假设imageBytes是包含图片数据的字节数组
        int readableBytes = byteBuf.readableBytes();
        byte[] imageBytes = new byte[readableBytes];
        len += readableBytes;
        byteBuf.readBytes(imageBytes);

        try {
            if (imageInProgress == null){
                ByteBuf buf = Unpooled.buffer(1024*1020);

                imageInProgress = new Image();
                imageInProgress.setBuf(buf);
                start = System.currentTimeMillis();
                System.out.println("----------创建图片对象,可读字节:"+imageInProgress.getBuf().readableBytes() +"-----------");
            }
            ByteBuf buf = imageInProgress.getBuf();

            byte[] lastThreeByte = Arrays.copyOfRange(imageBytes,imageBytes.length-3,imageBytes.length);
            String end = new String(lastThreeByte);

            if ("end".equals(end)){
                buf.writeBytes(imageBytes,0,imageBytes.length-3);
                imageInProgress.setLen(len-3);
            }else {
                buf.writeBytes(imageBytes);
                imageInProgress.setLen(len);
            }

//            System.out.println("可读字节:"+imageInProgress.getBuf().readableBytes() +" len长度:"+imageInProgress.getLen());


            if ("end".equals(end)){
                // 关闭输出流
                completeImage = new ImageResponse();
                int completeReadableBytes = imageInProgress.getBuf().readableBytes();
                byte[] bytes = new byte[completeReadableBytes];
                imageInProgress.getBuf().readBytes(bytes);

                completeImage.setBody(bytes);
                completeImage.setReadableBytes(completeReadableBytes);
                completeImage.setLen(imageInProgress.getLen());

                imageInProgress = null;

                endTime = System.currentTimeMillis();
                System.out.println("读到图片结尾， 图片大小:"+len +" 耗时："+(endTime - start));
                len = 0;

                // 创建一个文件输出流，用于写入图片数据
//                outputFile = new File("/Users/linsong.chen/Downloads/image/outputImage_"+index.getAndAdd(1)+".jpg");
//                os = new FileOutputStream(outputFile);
//                // 将字节流写入到文件中
//                os.write(bytes);
//                // 确保所有数据都被写入
//                os.flush();
            }
//            System.out.println("写入成功！大小:"+imageBytes.length+" 文件序号:"+index.get());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("图片写入失败！");
        }
    }

    private synchronized void writeToFile1(ByteBuf byteBuf){
        // 假设imageBytes是包含图片数据的字节数组
        int readableBytes = byteBuf.readableBytes();
        byte[] imageBytes = new byte[readableBytes];
        len += readableBytes;
        byteBuf.readBytes(imageBytes);

        try {
            if (imageInProgress == null){
                ByteBuf buf = Unpooled.buffer(1024);

                imageInProgress = new Image();
                imageInProgress.setBuf(buf);
                start = System.currentTimeMillis();
                System.out.println("----------创建图片对象,可读字节:"+imageInProgress.getBuf().readableBytes() +"-----------");
            }
            ByteBuf buf = imageInProgress.getBuf();

            byte[] lastThreeByte = Arrays.copyOfRange(imageBytes,imageBytes.length-3,imageBytes.length);
            String end = new String(lastThreeByte);

            if ("end".equals(end)){
                buf.writeBytes(imageBytes,0,imageBytes.length-3);
                imageInProgress.setLen(len-3);
            }else {
                buf.writeBytes(imageBytes);
                imageInProgress.setLen(len);
            }

//            System.out.println("可读字节:"+imageInProgress.getBuf().readableBytes() +" len长度:"+imageInProgress.getLen());


            if ("end".equals(end)){
                // 关闭输出流
                completeImage = new ImageResponse();
                int completeReadableBytes = imageInProgress.getBuf().readableBytes();
                byte[] bytes = new byte[completeReadableBytes];
                imageInProgress.getBuf().readBytes(bytes);

                completeImage.setBody(bytes);
                completeImage.setReadableBytes(completeReadableBytes);
                completeImage.setLen(imageInProgress.getLen());

                imageInProgress = null;

                endTime = System.currentTimeMillis();
                System.out.println("读到图片结尾， 图片大小:"+len +" 耗时："+(endTime - start));
                len = 0;
            }
//            System.out.println("写入成功！大小:"+imageBytes.length+" 文件序号:"+index.get());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("图片写入失败！");
        }
    }

    public class TestSub implements Subscriber{
        private CountDownLatch countDownLatch;
        public TestSub(){

        }

        public TestSub(CountDownLatch countDownLatch){
            this.countDownLatch = countDownLatch;
        }

        Subscription s;
        @Override
        public void onSubscribe(Subscription s) {
            System.out.println("TestSub Subscription:"+Thread.currentThread());
            this.s = s;
            s.request(1);
        }

        @Override
        public void onNext(Object o) {
            System.out.println("TestSub onNext:"+o);
            this.s.request(1);
        }


        @Override
        public void onError(Throwable t) {
            System.out.println("onError:"+t);
        }

        @Override
        public void onComplete() {
            System.out.println("TestSub onComplete");
            if (countDownLatch != null){
                countDownLatch.countDown();
            }
        }
    }

}
