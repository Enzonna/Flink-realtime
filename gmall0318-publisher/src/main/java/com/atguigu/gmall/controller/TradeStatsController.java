package com.atguigu.gmall.controller;


import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.service.TradeStatesService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

// 将当前类对象的创建交给Spring容器，如果类中方法返回的是String，直接响应给客户端
@RestController
public class TradeStatsController {

    @Autowired
    TradeStatesService tradeStatesService;

    // 拦截请求，将请求交给对应的方法进行处理
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            // 请求中如果没有携带查询的日期，将当天作为查询的日期
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatesService.getGMV(date);
        String json = "{\"status\":0,\"data\":" + gmv + "}";
        System.out.println("json = " + json);
        return json;
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceAmountList = tradeStatesService.getProvinceAmount(date);
        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceAmountList.get(i);
            jsonB.append("{\"name\": \"")
                    .append(provinceOrderAmount.getProvinceName())
                    .append("\",\"value\": ")
                    .append(provinceOrderAmount.getOrderAmount())
                    .append("}");
            if (i < provinceAmountList.size() - 1) {
                jsonB.append(",");
            }
        }
        jsonB.append("],\"valueName\": \"交易额\"}}");

        System.out.println("jsonB = " + jsonB);
        return jsonB.toString();
    }

}
