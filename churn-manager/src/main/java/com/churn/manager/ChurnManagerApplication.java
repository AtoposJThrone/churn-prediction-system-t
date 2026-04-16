package com.churn.manager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ChurnManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ChurnManagerApplication.class, args);
    }
}
