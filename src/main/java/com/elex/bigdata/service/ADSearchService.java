package com.elex.bigdata.service;

import com.elex.bigdata.hbase.HBaseUtil;
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

            scan.addFamily(cf);
            byte[][] startStopRow = getStartStopRow(pid,startTime,endTime,nation);
            scan.setStartRow(startStopRow[0]);
            scan.setStopRow(startStopRow[1]);
            scan.setCaching(1000);

            ResultScanner rs = hTable.getScanner(scan);
            int hit = 0;
            int miss = 0;
            int ab = 0;
            for (Result r : rs) {
                int game = Bytes.toInt(r.getColumnLatest(cf,a).getValue());
                int shop = Bytes.toInt(r.getColumnLatest(cf,b).getValue());
                int cat = Bytes.toInt(r.getColumnLatest(cf,c).getValue());

                if(game == shop){
                    ab++;
                }else if((game > shop && cat == 1) || (game < shop && cat == 2) ){
                    hit ++;
                }else{
                    miss ++;
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
        byte[] startRow = Bytes.add(new byte[]{(byte)pid},Bytes.toBytes(nation),Bytes.toBytes(startTime));
        byte[] stopRow = Bytes.add(new byte[]{(byte)pid},Bytes.toBytes(nation),Bytes.toBytes(endTime));
        return new byte[][]{startRow,stopRow};
    }

}
