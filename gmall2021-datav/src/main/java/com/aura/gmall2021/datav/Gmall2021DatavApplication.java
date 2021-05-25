package com.aura.gmall2021.datav;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.aura.gmall2021.datav.mapper")
public class Gmall2021DatavApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall2021DatavApplication.class, args);
    }

}
