package org.example.springwebflux.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * WebClient 配置
 * 
 * 用于调用外部服务的非阻塞 HTTP 客户端
 */
@Configuration
public class WebClientConfig {

    /**
     * 创建 WebClient Bean
     */
    @Bean
    public WebClient webClient(WebClient.Builder builder) {
        // 配置 Netty HttpClient
        HttpClient httpClient = HttpClient.create()
                // 连接超时
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                // 响应超时
                .responseTimeout(Duration.ofSeconds(5))
                // 读写超时
                .doOnConnected(conn -> 
                    conn.addHandlerLast(new ReadTimeoutHandler(5, TimeUnit.SECONDS))
                        .addHandlerLast(new WriteTimeoutHandler(5, TimeUnit.SECONDS))
                );

        return builder
                // 使用配置好的 HttpClient
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                // 可以设置基础 URL（可选）
                // .baseUrl("http://localhost:8080")
                .build();
    }
}

