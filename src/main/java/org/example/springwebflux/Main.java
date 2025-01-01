package org.example.springwebflux;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Author: linsong.chen@huolala.cn
 * @CreateTime: ${DATE} ${TIME}
 * @Description:
 */
@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        System.setProperty("io.netty.allocator.chunkSize", "32768");
        SpringApplication.run(Main.class);
    }
}