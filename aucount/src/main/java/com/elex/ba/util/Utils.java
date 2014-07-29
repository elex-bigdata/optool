package com.elex.ba.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午5:05
 */
public class Utils {

    public static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

    public static long transformerUID(byte[] hashUID){
        int offset = 5;
        byte[] newBytes = new byte[offset];
        System.arraycopy(hashUID, 0, newBytes, 0, offset);
        long samplingUid = 0;
        for (int i = 0; i < offset; i++) {
            samplingUid <<= 8;
            samplingUid ^= newBytes[i] & 0xFF;
        }
        return samplingUid;
    }


    //获取HBase查询的开始结束时间：20140714,7 将返回20140707,20140715
    public static String[] getDateRange(String date, int dayOffset) throws ParseException {

        String[] dateRange = new String[2];
        Date end = format.parse(date);

        Calendar cal = Calendar.getInstance();
        cal.setTime(end);

        cal.add(5,1);
        String endTime = format.format(cal.getTime());

        cal.add(5,-dayOffset);
        String startTime = format.format(cal.getTime());

        dateRange[0] = startTime;
        dateRange[1] = endTime;

        return dateRange;
    }

    public static String getLastDate(String date) throws ParseException{
        Date day = format.parse(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(day);

        cal.add(5,-1);
        return format.format(cal.getTime());
    }

    public static String[] getLastDate(String date,int num) throws ParseException{
        Date day = format.parse(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(day);



        String[] result = new String[num];
        result[0] = date;
        for(int i =1;i<num;i++){
            cal.add(5,-1);
            result[i] = format.format(cal.getTime());
        }

        return result;
    }

    public static String getHBaseUIDPath(String node,String pid){
        return "/user/hadoop/offline/node/" + node + "/" + pid;
    }

    public static String getHBaseUIDPath(String date,String node,String pid){
        return "/user/hadoop/offline/node/" + node + "/" + pid + "/" + date + "-r-0000";
    }

    public static String getUIDCombinePath(String date,String pid){
        return "/user/hadoop/offline/"+date+"/pid/" +  pid;
    }

    public static String getProjectCombinePath(String date,String project){
        return "/user/hadoop/offline/"+date+"/combine/" +  project;
    }

    public static String getProjectCountPath(String date,String project, int range){
        return "/user/hadoop/offline/"+date+"/count" + range + "/" +  project;
    }

    public static void main(String[] args) throws ParseException {
       /* String[]  week = getDateRange("20140711",7);
        System.out.println(week[0] + " " + week[1]);
        String[] month = getDateRange("20140711",30);
        System.out.println(month[0] + " " + month[1]);*/
        String[] days = Utils.getLastDate("20140728",30);
        System.out.println(days[0]);
        System.out.println(days[29]);
    }

}
