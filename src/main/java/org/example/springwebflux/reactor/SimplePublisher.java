package org.example.springwebflux.reactor;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/12/1 23:42
 * @Description:
 */
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimplePublisher implements Publisher<String> {
    private final String[] data;
    private final ExecutorService executor = Executors.newCachedThreadPool(); // 用于异步发送数据

    public SimplePublisher(String[] data) {
        this.data = data;
    }

    @Override
    public void subscribe(Subscriber<? super String> subscriber) {
        subscriber.onSubscribe(new Subscription() {
            private int index = 0; // 当前发送的数据索引
            private boolean completed = false;

            @Override
            public void request(long n) {
                if (n <= 0) {
                    subscriber.onError(new IllegalArgumentException("Request must be positive"));
                } else {
                    for (long i = 0; i < n && index < data.length && !completed; i++) {
                        subscriber.onNext(data[index++]);
                    }
                    if (index == data.length) {
                        onComplete();
                    }
                }
            }

            @Override
            public void cancel() {
                // 这里可以添加取消订阅后的清理逻辑
            }
        });

        // 可以在这里立即请求数据或根据需要异步请求
//        subscriber.request(1); // 初始请求一个数据项
    }

    private void onComplete() {
        System.out.println("完成");
    }
}
