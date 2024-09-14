package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TrafficUvCt;

import java.util.List;

public interface TrafficStatsService {
    List<TrafficUvCt> getChUvCt(Integer date, Integer limit);
}
