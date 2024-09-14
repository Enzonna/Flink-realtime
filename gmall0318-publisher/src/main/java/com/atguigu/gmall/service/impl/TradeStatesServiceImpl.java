package com.atguigu.gmall.service.impl;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.mapper.TradeStateMapper;
import com.atguigu.gmall.service.TradeStatesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;


@Service
public class TradeStatesServiceImpl implements TradeStatesService {
    @Autowired
    TradeStateMapper tradeStateMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStateMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStateMapper.selectProvinceAmount(date);
    }
}
