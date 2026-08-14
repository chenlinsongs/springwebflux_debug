package org.example.springwebflux.debug;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

/**
 * 启动调试监听器
 * 
 * 用于监听Spring Boot的启动过程，验证启动顺序
 */
@Component
public class StartupDebugListener implements ApplicationListener<ApplicationEvent> {

    private static final Logger logger = LoggerFactory.getLogger(StartupDebugListener.class);
    
    private static int step = 1;
    
    // 生成重复字符串的方法（兼容Java 8）
    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        String separator = repeatString("=", 80);
        
        if (event instanceof ContextRefreshedEvent) {
            // Spring容器刷新完成（IoC、AOP完成）
            logger.info("\n" + separator);
            logger.info("【步骤{}】Spring容器刷新完成", step++);
            logger.info("说明：IoC和AOP已经完成，所有Bean已创建");
            logger.info("此时映射表已生成，但Netty还未启动");
            logger.info(separator);
            
        } else if (event instanceof WebServerInitializedEvent) {
            // Web服务器启动完成（Netty启动完成）
            WebServerInitializedEvent webEvent = (WebServerInitializedEvent) event;
            logger.info("\n" + separator);
            logger.info("【步骤{}】Netty服务器启动完成", step++);
            logger.info("端口: {}", webEvent.getWebServer().getPort());
            logger.info("说明：Netty已启动，ReactorHttpHandlerAdapter已注入");
            logger.info("现在可以接收请求了！");
            logger.info(separator);
            
        } else if (event instanceof ApplicationReadyEvent) {
            // 应用完全启动
            logger.info("\n" + separator);
            logger.info("【步骤{}】应用完全启动", step++);
            logger.info("说明：一切就绪，可以处理请求");
            logger.info(separator + "\n");
        }
    }
}

