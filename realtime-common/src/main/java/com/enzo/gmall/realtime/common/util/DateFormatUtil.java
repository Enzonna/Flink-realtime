package com.enzo.gmall.realtime.common.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 *  注意：在进行日期类型转换的时候，尽量不要使用SimpleDateFormat，因为底层存在线程安全的问题
 *  建议使用jdk1.8日期包下的相关类
 */
public class DateFormatUtil {
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter dtfForPartition = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter dtfFull = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 2023-07-05 01:01:01 转成 ms 值
     * @param dateTime
     * @return
     */
    public static Long dateTimeToTs(String dateTime) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, dtfFull);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
    
    /**
     * 把毫秒值转成 年月日:  2023-07-05
     * @param ts
     * @return
     */
    public static String tsToDate(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }
    
    /**
     * 把毫秒值转成 年月日时分秒:  2023-07-05 01:01:01
     * @param ts
     * @return
     */
    public static String tsToDateTime(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfFull.format(localDateTime);
    }
    
    public static String tsToDateForPartition(long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtfForPartition.format(localDateTime);
    }
    
    /**
     * 把 年月日转成 ts
     * @param date
     * @return
     */
    public static long dateToTs(String date) {
        return dateTimeToTs(date + " 00:00:00");
    }
}