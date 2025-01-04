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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.*;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
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

import java.awt.image.ImageConsumer;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
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
    Logger logger = LoggerFactory.getLogger(FlushResponseController.class);

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

    @GetMapping("/upload2/{name}")
    public Mono<String> upload2(@PathVariable String name) throws InterruptedException {
        if (myConsumer != null){
            myConsumer.sendMessage(name);
            return Mono.just("成功");
        }else {
            return Mono.just("对方未上线");
        }
    }

    @GetMapping("/upload2/finish")
    public Mono<String> finish() throws InterruptedException {
        if (myConsumer != null || imageConsumer != null){
            if (myConsumer != null){
                myConsumer.finish();
                myConsumer = null;
            }
            if (imageConsumer != null){
                imageConsumer.finish();
                imageConsumer = null;
            }
            return Mono.just("成功返回");
        } else {
            return Mono.just("对方未上线");
        }
    }

    BlockingQueue<ImageResponse> queue = new LinkedBlockingQueue<>();

    BlockingQueue<String> queueTest = new LinkedBlockingQueue<>();
    Flux fluxTest;
    public Flux createFluxFromQueue1() {

        Flux flux = Flux.generate(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return 0;
            }
        }, new BiFunction<Integer, SynchronousSink<DataBuffer>, Integer>() {
            @Override
            public Integer apply(Integer state, SynchronousSink<DataBuffer> sink) {
                if (state < 5) {
                    DataBuffer bytes = bufferFactory.wrap(("Data " + state).getBytes());
                    sink.next(bytes);
                    return state + 1;
                } else {
                    sink.complete();
                    return state;
                }
            }

        });
        return flux;
    }

    MyConsumer myConsumer;
    public Flux createFluxFromQueue() {
        myConsumer = new MyConsumer();
        Flux flux = Flux.create(myConsumer);
        return flux;
    }

    private class MyConsumer implements Consumer<FluxSink<DataBuffer>>{

        private FluxSink<DataBuffer> fluxSink;
        @Override
        public void accept(FluxSink<DataBuffer> fluxSink) {
            this.fluxSink = fluxSink;
        }

        public void sendMessage(String message){
            DataBuffer bytes = bufferFactory.wrap(("Data " + message).getBytes());
            fluxSink.next(bytes);
        }

        public void finish(){
            fluxSink.complete();
        }
    }


    ImageConsumer imageConsumer;
    public Flux createImageFlux() {
        imageConsumer = new ImageConsumer();
        Flux flux = Flux.create(imageConsumer);
        return flux;
    }


    private class ImageConsumer implements Consumer<FluxSink<DataBuffer>>{

        private FluxSink<DataBuffer> fluxSink;
        @Override
        public void accept(FluxSink<DataBuffer> fluxSink) {
            this.fluxSink = fluxSink;
        }

        public void sendMessage(ImageResponse message){
            DataBuffer bytes = bufferFactory.wrap(message.getBody());
            fluxSink.next(bytes);
        }

        public void finish(){
            fluxSink.complete();
        }
    }




    Mono<ImageResponse> bodyProducer;
    DataBufferFactory bufferFactory = new DefaultDataBufferFactory();
    @GetMapping("/download2")
    public Mono<Void> download2(ServerWebExchange exchange){

        Mono<Void> mono = Mono.defer(() -> {
            Flux<DataBuffer> body = createFluxFromQueue();
            return exchange.getResponse().writeWith(body);
        });
        return mono;
    }

    @GetMapping("/download3")
    public Mono<Void> download3(ServerWebExchange exchange){

        Mono<Void> mono = Mono.defer(() -> {
            Flux<DataBuffer> body = createImageFlux();
            return exchange.getResponse().writeWith(body);
        });
        return mono;
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
    int count = 0;
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
//                System.out.println("----------创建图片对象,可读字节:"+imageInProgress.getBuf().readableBytes() +"-----------");
            }
            ByteBuf buf = imageInProgress.getBuf();

            byte[] lastThreeByte = Arrays.copyOfRange(imageBytes,imageBytes.length-3,imageBytes.length);
            String end = new String(lastThreeByte);

            if ("end".equals(end)){
                byte[] countByte = Arrays.copyOfRange(imageBytes,imageBytes.length-3-4,imageBytes.length-3);
                count = byteArrayToInt(countByte);
                buf.writeBytes(imageBytes,0,imageBytes.length-3-4);
                imageInProgress.setLen(len-3-4);
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
                logger.info("读到图片结尾， 图片大小:"+len +" 耗时："+(endTime - start)+" 顺序："+count);
//                System.out.println("读到图片结尾， 图片大小:"+len +" 耗时："+(endTime - start)+" 顺序："+count);
                len = 0;
                if (imageConsumer != null){
                    imageConsumer.sendMessage(completeImage);
                }

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

    public static byte[] intToByteArray(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >>> 24); // 获取最高8位
        bytes[1] = (byte) (value >>> 16); // 获取次高8位
        bytes[2] = (byte) (value >>> 8);  // 获取次低8位
        bytes[3] = (byte) (value);       // 获取最低8位
        return bytes;
    }

    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;
        value |= (bytes[0] & 0xFF) << 24; // 将第一个字节左移24位
        value |= (bytes[1] & 0xFF) << 16; // 将第二个字节左移16位
        value |= (bytes[2] & 0xFF) << 8;  // 将第三个字节左移8位
        value |= (bytes[3] & 0xFF);       // 将第四个字节保持不变
        return value;
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
