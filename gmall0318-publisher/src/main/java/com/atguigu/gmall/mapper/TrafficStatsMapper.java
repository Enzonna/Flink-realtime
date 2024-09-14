package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 流量域统计mapper接口
 */
@Mapper
public interface TrafficStatsMapper {
    // 获取某天各个渠道独立访客排名前几
    @Select("select ch,sum(uv_ct) uv_ct from dws_traffic_vc_ch_ar_is_new_page_view_window partition par#{date}" +
            " group by ch order by uv_ct desc limit #{limit}")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
