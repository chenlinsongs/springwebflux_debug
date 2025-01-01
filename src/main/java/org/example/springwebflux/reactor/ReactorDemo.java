package org.example.springwebflux.reactor;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/12/1 15:12
 * @Description:
 */
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactorDemo {
    public static void main(String[] args) {
        Mono<String> mono = Mono.just("Hello, Mono!");
        mono.subscribe(System.out::println);

        Flux<String> flux = Flux.just("Hello,", "Flux!");
        flux.subscribe(System.out::println);
    }
}
