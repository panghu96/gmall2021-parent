package com.aura.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.aura.gmall.publisher.service.ESService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author:panghu
 * Date:2021-05-17
 * Description:
 */
@RestController
public class PublisherController {
    @Autowired
    ESService esService;

    @RequestMapping(value = "/realtime-total", method = RequestMethod.GET)
    public String realtimeTotal(@RequestParam("date") String dt) {
        //获取日活数
        Long dauTotal = esService.getDauTotal(dt);
        List<Map<String, Object>> dauTotalList = new ArrayList<>();
        Map<String, Object> newDauAct = new HashMap<>();
        newDauAct.put("id", "dau");
        newDauAct.put("name", "新增日活");
        if (dauTotalList != null) {
            newDauAct.put("value", dauTotal);
        } else {
            newDauAct.put("value", 0);
        }
        Map<String, Object> newDauMid = new HashMap<>();
        newDauMid.put("id", "new_mid");
        newDauMid.put("name", "新增设备");
        newDauMid.put("value", 233);
        dauTotalList.add(newDauAct);
        dauTotalList.add(newDauMid);
        //转为JSON字符串
        return JSON.toJSONString(dauTotalList);
    }

    @GetMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id") String id, @RequestParam("date") String dt) {
        Map todayDauHour = esService.getDauHour(dt);
        //获取昨天的每小时日活
        String yesterday = getYesterday(dt);
        Map yesterdayDauHour = esService.getDauHour(yesterday);
        Map<String, Map<String, Long>> dauHourMap = new HashMap<>();
        dauHourMap.put("yesterday", yesterdayDauHour);
        dauHourMap.put("today", todayDauHour);

        return JSON.toJSONString(dauHourMap);
    }

    /**
     * 获取昨天日期
     *
     * @param dt
     * @return
     */
    private String getYesterday(String dt) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date today = dateFormat.parse(dt);
            Date yesterday = DateUtils.addDays(today, -1);
            return dateFormat.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期不规范！");
        }
    }


}
