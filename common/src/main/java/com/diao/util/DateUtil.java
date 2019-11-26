package com.diao.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    private DateUtil(){}

    /**
     * 根据指定格式将日期对象转换为字符串
     * @param d
     * @param format
     * @return
     */
    public static String getDateFormat(Date d, String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(d);
    }

    /**
     * 将字符串按指定格式转为日期对象
     * @param dateString
     * @param format
     * @return
     * @throws ParseException
     */
    public static Date parse(String dateString,String format) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.parse(dateString);
    }
}
