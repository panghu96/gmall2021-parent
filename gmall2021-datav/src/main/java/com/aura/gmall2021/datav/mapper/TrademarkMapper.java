package com.aura.gmall2021.datav.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * Author:panghu
 * Date:2021-05-25
 * Description:
 */
public interface TrademarkMapper {
    //查询热门品牌  参数别名用于和mapper.xml中#{参数名}匹配
    public List<Map> selectTrademarkAmount(@Param("startDate") String startDate,
                                           @Param("endDate") String endDate,
                                           @Param("topN") Integer topN);
}
