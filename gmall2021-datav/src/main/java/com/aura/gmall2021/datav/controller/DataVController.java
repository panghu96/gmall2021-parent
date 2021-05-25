package com.aura.gmall2021.datav.controller;

import com.alibaba.fastjson.JSON;
import com.aura.gmall2021.datav.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Author:panghu
 * Date:2021-05-25
 * Description:
 */
@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @RequestMapping("trademark")
    public String trademarkSum(@RequestParam("startDate") String startDate,
                               @RequestParam("endDate") String endDate,
                               @RequestParam("topN") Integer topN) {

        List<Map> trademarkAmount = mySQLService.getTrademarkAmount(startDate, endDate, topN);
        return JSON.toJSONString(trademarkAmount);
    }
}
