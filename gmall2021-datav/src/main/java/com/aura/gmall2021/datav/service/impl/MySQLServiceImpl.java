package com.aura.gmall2021.datav.service.impl;


import com.aura.gmall2021.datav.mapper.TrademarkMapper;
import com.aura.gmall2021.datav.service.MySQLService;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

/**
 * Author:panghu
 * Date:2021-05-25
 * Description:
 */
@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TrademarkMapper trademarkMapper;

    @Override
    public List<Map> getTrademarkAmount(String startDate, String endDate, Integer topN) {

        return trademarkMapper.selectTrademarkAmount(startDate,endDate,topN);
    }
}
