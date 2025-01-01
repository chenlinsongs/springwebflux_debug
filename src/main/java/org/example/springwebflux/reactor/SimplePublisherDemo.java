package org.example.springwebflux.reactor;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/12/1 23:43
 * @Description:
 */
public class SimplePublisherDemo {
    public static void main(String[] args) {
        SimplePublisher publisher = new SimplePublisher(new String[]{"Hello", "World"});
        publisher.subscribe(new Subscriber<String>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE); // 请求所有数据
            }

            @Override
            public void onNext(String s) {
                System.out.println(s);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("Completed");
            }
        });
    }
}
