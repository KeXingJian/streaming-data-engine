package com.kxj.streamingdataengine;

import com.kxj.streamingdataengine.demo.StreamingDemo;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class StreamingDataEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamingDataEngineApplication.class, args);
    }

    @Bean
    public CommandLineRunner run() {
        return args -> {
            System.out.println("=== 高性能流式数据处理引擎启动 ===");
            System.out.println("运行演示请访问: http://localhost:8080/demo");
        };
    }

}
