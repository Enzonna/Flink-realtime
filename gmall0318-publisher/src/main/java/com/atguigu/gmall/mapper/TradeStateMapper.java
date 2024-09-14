package com.atguigu.gmall.mapper;


import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * 交易域统计mapper接口
 */

@Mapper
public interface TradeStateMapper {
    // 获取某天总交易额
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);


    // 获取某天各个省份的交易额
    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window " +
            "partition par#{date} group by province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}



/*
class MyC implements TradeStateMapper {

    @Override
    public BigDecimal selectGMV(Integer date) {
        BigDecimal bigDecimal = null;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        // jdbc
        try {
            // 注册驱动
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 建立连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:9030/gmall_realtime?user=root&password=aaaaaa");

            // 获取数据库操作对象
            String sql = "select sum(order_amount) order_amount from dws_trade_province_order_window partition par" + date;
            ps = conn.prepareStatement(sql);

            // 执行SQL语句
            rs = ps.executeQuery();
            rs.next();
            bigDecimal = rs.getBigDecimal(1);

            // 处理结果集

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 释放资源
        }

        return bigDecimal;
    }
}
*/