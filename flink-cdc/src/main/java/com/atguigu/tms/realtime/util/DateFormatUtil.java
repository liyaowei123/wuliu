package com.atguigu.tms.realtime.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {
    // yyyy-MM-dd HH:mm:ss 日期格式化对象
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将 yyyy-MM-dd HH:mm:ss 格式化日期字符串转换为毫秒时间戳
     * @param dtStr yyyy-MM-dd HH:mm:ss 格式化日期字符串
     * @return 毫秒时间戳
     */
    public static Long toTs(String dtStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(dtStr, dtf);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    /**
     * 将毫秒时间戳转换为 yyyy-MM-dd HH:mm:ss 格式化日期字符串
     * @param ts 毫秒时间戳
     * @return  yyyy-MM-dd HH:mm:ss 格式化日期字符串
     */
    public static String toYmdHms(Long ts) {
        Date dt = new Date(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(dt.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }
}
