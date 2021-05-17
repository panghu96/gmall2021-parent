package com.aura.gmall.publisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Author:panghu
 * Date:2021-05-17
 * Description:
 */
public interface ESService {
    public Long getDauTotal(String date);

    public Map getDauHour(String date);
}
