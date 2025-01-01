package org.example.springwebflux.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.boot.web.embedded.netty.NettyServerCustomizer;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.netty.http.server.HttpServer;

@Configuration
public class NettyConfig {

    @Bean
    public WebServerFactoryCustomizer serverFactoryCustomizer() {
        return new NettyTimeoutCustomizer();
    }

    class NettyTimeoutCustomizer implements WebServerFactoryCustomizer<NettyReactiveWebServerFactory> {

        @Override
        public void customize(NettyReactiveWebServerFactory factory) {
            int connectionTimeout = 1;
            int writetimeout = 3;
            factory.addServerCustomizers(server -> server.tcpConfiguration(tcp ->
                    tcp.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeout)

                            .doOnConnection(connection ->
                                    connection.addHandlerLast(new WriteTimeoutHandler(writetimeout)))));
            factory.addServerCustomizers(new NettyServerCustomizer() {
                @Override
                public HttpServer apply(HttpServer httpServer) {
                    httpServer.option(ChannelOption.SO_RCVBUF,160*1024);
                    httpServer.option(ChannelOption.SO_SNDBUF,160*1024);
                    return httpServer;
                }
            });
        }
    }

}
