package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

public interface TradeStatesService {
    // 获取某天总交易额
    BigDecimal getGMV(Integer date);

    // 获取某天交易省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
