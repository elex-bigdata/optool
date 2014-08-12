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

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static long toLong(byte[] bytes) throws Exception {
        return toLong(bytes, 0, 8);
    }

    public static long toLong(byte[] bytes, int offset, final int length) throws Exception {
        if (length != 8 || offset + length > bytes.length) {
            throw new Exception(
                    "to long exception " + "offset " + offset + " length " + length + " bytes.len " + bytes.length);
        }
        long l = 0;
        for (int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    public static Long truncate(long hashedUID) throws Exception {
        byte[] bytes, newBytes;
        bytes = toBytes(hashedUID);
        newBytes = new byte[bytes.length];
        System.arraycopy(bytes, 4, newBytes, 4, 4);
        return toLong(newBytes);
    }

    public static String getHBaseUIDPath(String node,String pid){
        return "/user/hadoop/offline/node/" + node + "/" + pid;
    }

    public static String getMysqlAttrPath(String node,String pid,String attr){
        return "user/hadoop/mysql/"+pid+"/"+node+"_"+attr+".log";
    }

    public static String getHBaseUIDPath(String date,String node,String pid){
        return "/user/hadoop/offline/node/" + node + "/" + pid + "/" + date + "-r-0000";
    }

    public static String getUIDCombinePath(String pid){
        return "/user/hadoop/offline/pid/" +  pid;
    }

    public static String getProjectCombinePath(String project){
        return "/user/hadoop/offline/combine/" +  project;
    }

    public static String getProjectCountPath(String date,String project, int range){
        return "/user/hadoop/offline/"+date+"/count" + range + "/" +  project;
    }

    public static String getRegistPath(String date,String project){
        return "/user/hadoop/offline/regist/"+date+"/" +  project;
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
