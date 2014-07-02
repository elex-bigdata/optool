package com.elex.bigdata.service;

import com.elex.bigdata.hbase.HBaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Author: liqiang
 * Date: 14-4-3
 * Time: 下午6:17
 */
public class ADSearchService {

    public String countHit(String tableName, int pid, Long startTime, Long endTime, String nation ) throws Exception{
        HTableInterface hTable = null;
        try{
            hTable = HBaseUtil.getHTable(tableName);
            Scan scan = new Scan();
            byte[] cf = Bytes.toBytes("h");
            byte[] a = Bytes.toBytes("a");
            byte[] b = Bytes.toBytes("b");
            byte[] c = Bytes.toBytes("c");
            byte[] t = Bytes.toBytes("t");

            scan.addFamily(cf);
            byte[][] startStopRow = getStartStopRow(pid,startTime,endTime,nation);
            scan.setStartRow(startStopRow[0]);
            scan.setStopRow(startStopRow[1]);
            scan.setCaching(1000);
            scan.setTimeRange(startTime,endTime);


            ResultScanner rs = hTable.getScanner(scan);
            int hit = 0;
            int miss = 0;
            int ab = 0;
            //0，未指定 1，游戏 2，电商 99，其他
            for (Result r : rs) {
                KeyValue kv = r.getColumnLatest(cf,t);
                int cat = Bytes.toInt(r.getColumnLatest(cf,c).getValue());
                if(kv == null){
                    int game = Bytes.toInt(r.getColumnLatest(cf,a).getValue());
                    int shop = Bytes.toInt(r.getColumnLatest(cf,b).getValue());

                    if(game == shop){
                        ab++;
                    }else if((game > shop && cat == 1) || (game < shop && cat == 2) ){
                        hit ++;
                    }else{
                        miss ++;
                    }
                }else{
                    //b.19,a.21,z.60 a.游戏 b.电商 z.未知
                    String tStr = Bytes.toString(r.getColumnLatest(cf,t).getValue());
                    if(("a".equals(tStr) && cat ==1) || ("b".equals(tStr) && cat == 2)){
                        hit ++;
                    }else if("z".equals(tStr)){
                        ab ++;
                    }else{
                        miss ++;
                    }
                }


            }
            return "ab:" + ab + ", hit:" + hit + ", miss:" + miss ;
        }catch (Exception e){
            throw e;
        }finally {
            if(hTable != null){
                hTable.close();
            }
        }
    }

    public int count(String tableName, int pid, Long startTime, Long endTime, String nation ) throws Exception{
        HTableInterface hTable = null;
        try{
            hTable = HBaseUtil.getHTable(tableName);
            Scan scan = new Scan();
            byte[][] startStopRow = getStartStopRow(pid,startTime,endTime,nation);
            scan.setStartRow(startStopRow[0]);
            scan.setStopRow(startStopRow[1]);
            scan.setFilter(new KeyOnlyFilter());
            scan.setCaching(1000);
            scan.setTimeRange(startTime,endTime);

            ResultScanner rs = hTable.getScanner(scan);
            int count = 0;
            for (Result r : rs) {
                count ++;
            }
            return count ;
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
