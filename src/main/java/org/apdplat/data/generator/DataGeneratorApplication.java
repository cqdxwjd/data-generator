package org.apdplat.data.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class DataGeneratorApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DataGeneratorApplication.class, args);
    }

    @PostConstruct
    public void run() {
        LOGGER.info("开始生成数据");
    }
}
