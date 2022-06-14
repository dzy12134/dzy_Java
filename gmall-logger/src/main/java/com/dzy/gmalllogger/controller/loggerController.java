package com.dzy.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController
@Slf4j
public class loggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test1")
    public String test1(){
        System.out.println("success");
        return "success";
    }
    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn, @RequestParam(value = "age", defaultValue ="18") int age){
        System.out.println(nn+":"+age);
        return "success";
    }
    @RequestMapping("applog")
    public String getLog(@RequestParam("param")String jsonStr){
        //打印数据
//        System.out.println(jsonStr);
        //数据落盘
        log.info(jsonStr);
        //数据写入Kafka
        kafkaTemplate.send("ods_base_log",jsonStr);
        return "success";
    }
}
