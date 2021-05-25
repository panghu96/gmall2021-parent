package com.aura.gmall2021.datav.service;

import java.util.List;
import java.util.Map;

/**
 * Author:panghu
 * Date:2021-05-25
 * Description:
 */
public interface MySQLService {
    //获取热门品牌统计结果
    public List<Map> getTrademarkAmount(String startDate, String endDate, Integer topN);
}
