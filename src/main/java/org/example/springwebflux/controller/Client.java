package org.example.springwebflux.controller;

import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.Optional;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: 2024/11/23 23:50
 * @Description:
 */
//@Configuration
public class Client {
    @Bean
    WebClient webClient(WebClient.Builder builder) {
        final HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(5))
                .keepAlive(true);

        return builder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
//                .filter(log())
                .build();
    }

    private ExchangeFilterFunction log() {
        return (request, next) -> {
            return logRequest(request, null, null, null)
                    .flatMap(next::exchange)
                    .flatMap(clientResponse -> logResponse(clientResponse, null, null, null));
        };
    }

    private Mono<ClientRequest> logRequest(ClientRequest clientRequest, Optional<Object> trace, Optional<Object> su, Optional<Object> logId) {
        return Mono.just(clientRequest);
    }

    private Mono<ClientResponse> logResponse(ClientResponse clientResponse, Optional<Object> trace, Optional<Object> su, Optional<Object> logId) {
        return Mono.just(clientResponse);
    }
}
