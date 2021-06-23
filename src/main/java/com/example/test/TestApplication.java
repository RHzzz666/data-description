package com.example.test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import gala2.read;
import javax.annotation.PostConstruct;

@SpringBootApplication
public class TestApplication {

    public static void main(String[] args)
    {
        SpringApplication.run(TestApplication.class, args);
    }
    @PostConstruct
    public void init() {
        read.Init();
        System.out.println("init...");
    }

}
