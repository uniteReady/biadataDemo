package com.bigdata.hadoop_offline.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/***
 * 将时间字符串格式化成新的格式
 */

public  class DateUtil {

    public  static String  parseDate(SimpleDateFormat oldFomat,SimpleDateFormat newFormat,String date) throws ParseException {

        Date parsedDate = oldFomat.parse(date);
        String newFormatedDate = newFormat.format(parsedDate);

        return newFormatedDate;

    }


}
