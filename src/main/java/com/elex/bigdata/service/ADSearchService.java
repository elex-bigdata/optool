package com.elex.bigdata.service;

import com.elex.bigdata.hbase.HBaseUtil;
import com.elex.bigdata.util.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Author: liqiang
 * Date: 14-4-3
 * Time: 下午6:17
 */
public class ADSearchService {

    public static final Log LOG = LogFactory.getLog(ADSearchService.class);

    public Map countHit(String tableName, int pid, Long startTime, Long endTime, String nation, boolean debug ) throws Exception{
        HTableInterface hTable = null;
        try{
            hTable = HBaseUtil.getHTable(tableName);
            Scan scan = new Scan();
            byte[] cf = Bytes.toBytes("h");
            byte[] a = Bytes.toBytes("a");
            byte[] b = Bytes.toBytes("b");
            byte[] c = Bytes.toBytes("c");
            byte[] d = Bytes.toBytes("d");
            byte[] t = Bytes.toBytes("t");

            Map<String,Integer> catMap = new HashMap<String,Integer>();
            catMap.put("a", 1);
            catMap.put("b", 2);
            catMap.put("d",4);

            scan.addFamily(cf);
            byte[][] startStopRow = getStartStopRow(pid,startTime,endTime,nation);
            scan.setStartRow(startStopRow[0]);
            scan.setStopRow(startStopRow[1]);
            scan.setCaching(1000);
            scan.setTimeRange(startTime,endTime);
            scan.addColumn(cf,t);
            scan.addColumn(cf,c);

            ResultScanner rs = hTable.getScanner(scan);
            int hit = 0;
            int miss = 0;
            //0，未指定 1，游戏 2，电商 99，其他
            List<String> debugLines = new ArrayList<String>();
            for (Result r : rs) {

//                KeyValue kv = r.getColumnLatest(cf,t);
                int cat = Bytes.toInt(r.getColumnLatest(cf,c).getValue());
                /*if(kv == null){
                    int game = Bytes.toInt(r.getColumnLatest(cf,a).getValue());
                    int shop = Bytes.toInt(r.getColumnLatest(cf,b).getValue());
                    int social = Bytes.toInt(r.getColumnLatest(cf,d).getValue());

                    String max = game > shop ?  (game > social ? "a" : "d") : (shop > social ? "b" : "d") ;

                    if(catMap.get(max) == cat){
                        hit ++;
                    }else{
                        miss++;
                    }

                }else{*/
                //1.100
                String tStr = Bytes.toString(r.getColumnLatest(cf,t).getValue()).split("\\.")[0];
                if(debug){
                    String uid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length - 11));
                    debugLines.add(uid + " " + Constant.dfmt.format(new Date(r.getColumnLatest(cf,c).getTimestamp())) + " " + tStr + "," + cat);
                }
                if(tStr.equals(String.valueOf(cat))){
                    hit ++;
                }else{
                    miss ++;
                }
//                }


            }
            Map result = new HashMap();
            result.put("count"," hit:" + hit + ", miss:" + miss );
            result.put("hit",debugLines);

            return result;
        }catch (Exception e){
            throw e;
        }finally {
            if(hTable != null){
                hTable.close();
            }
        }
    }

    public Map count(String tableName, int pid, Long startTime, Long endTime, String nation, boolean debug ) throws Exception{
        HTableInterface hTable = null;
        byte[] fm = Bytes.toBytes("basis");
        byte[] qf = Bytes.toBytes("c");
        byte[] aidCol = Bytes.toBytes("aid");
        byte[] widthCol = Bytes.toBytes("w");
        byte[] heightCol = Bytes.toBytes("h");
        try{
            hTable = HBaseUtil.getHTable(tableName);
            Scan scan = new Scan();
            byte[][] startStopRow = getStartStopRow(pid,startTime,endTime,nation);
            scan.setStartRow(startStopRow[0]);
            scan.setStopRow(startStopRow[1]);
//            scan.setFilter(new KeyOnlyFilter());
            scan.addFamily(fm);

            scan.setCaching(1000);
            scan.setTimeRange(startTime, endTime);



            ResultScanner rs = hTable.getScanner(scan);
            int count = 0;
            if(debug){
                LOG.debug("------" + pid + " all-------------");
            }
            List<String> debugLines = new ArrayList<String>();
            for (Result r : rs) {
                if(debug){
                    String uid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length - 11));
                    long time = r.getColumnLatest(fm,qf).getTimestamp();
//                    long time = Bytes.toLong(Bytes.head(Bytes.tail(r.getRow(),r.getRow().length-3),8));
                    String aid = Bytes.toString(r.getColumnLatest(fm, aidCol).getValue());

                    String detail = "{";
                    if(r.getColumnLatest(fm, widthCol) != null){
                        detail += "w:" + Bytes.toString(r.getColumnLatest(fm, widthCol).getValue()) + ",";
                    }

                    if(r.getColumnLatest(fm, heightCol) != null){
                        detail += "h:" + Bytes.toString(r.getColumnLatest(fm, heightCol).getValue());
                    }
                    detail += "}";

                    debugLines.add(uid + " " + Constant.dfmt.format(new Date(time)) + " " + Bytes.toString(r.getColumnLatest(fm,qf).getValue())
                            + " " + aid + ":" + detail);
                }

                count ++;
            }
            Map result = new HashMap();
            result.put("count",count);
            result.put("all",debugLines);
            rs.close();
            return result ;
        }catch (Exception e){
            throw e;
        }finally {
            if(hTable != null){
                hTable.close();
            }
        }
    }


    private byte[][] getStartStopRow(int pid, Long startTime, Long endTime, String nation){
        //Bytes.add(new byte[]{pid},Bytes.toBytes("BR"),Bytes.toBytes(start))
        System.out.println(startTime + " : " + endTime + " : " + nation + " : " + pid);
        byte[] startRow = null;
        byte[] stopRow = null;
        if(StringUtils.isBlank(nation)){
            startRow = new byte[]{(byte)pid};
            stopRow = new byte[]{(byte)(pid+1)};
        }else{
            startRow = Bytes.add(new byte[]{(byte)pid},Bytes.toBytes(nation),Bytes.toBytes(startTime));
            stopRow = Bytes.add(new byte[]{(byte)pid},Bytes.toBytes(nation),Bytes.toBytes(endTime));
        }


        return new byte[][]{startRow,stopRow};
    }

}
