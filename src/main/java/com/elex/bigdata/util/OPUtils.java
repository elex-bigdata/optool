package com.elex.bigdata.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: liqiang
 * Date: 14-8-4
 * Time: 上午11:01
 */
public class OPUtils {

    public static String dateToStr(Date date,SimpleDateFormat sdf){
        return sdf.format(date);
    }
}
