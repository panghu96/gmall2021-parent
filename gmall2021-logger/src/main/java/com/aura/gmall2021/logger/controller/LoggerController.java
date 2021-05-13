package com.aura.gmall2021.logger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * Author:panghu
 * Date:2021-05-06
 * Description:
 */
@RestController
@Slf4j
public class LoggerController {
    @Autowired
    KafkaTemplate kafka;

    @RequestMapping("/applog")
    public String appLog(@RequestBody String logString) {
        log.info(logString);
        //分流
        JSONObject jsonObject = JSONObject.parseObject(logString);
        //启动日志
        if (jsonObject.getString("start") != null && jsonObject.getString("start").length() > 0) {
            kafka.send("GMALL2021_START_LOG", jsonObject.getString("mid"), logString);
        } else {
            //事件日志
            kafka.send("GMALL2021_EVENT_LOG", jsonObject.getString("mid"), logString);
        }
        return "success";
    }
}
