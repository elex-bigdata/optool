package com.elex.ba.main;

import com.elex.ba.job.UIDCombineJob;
import com.elex.ba.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-8-5
 * Time: 下午6:51
 */
public class ScanHBaseUID {

    public static void main(String[] args){

        String pid = args[0];
        String startKey = args[1];
        String endKey = args[2];



        ExecutorService service = new ThreadPoolExecutor(25,40,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<List<String>>> tasks = new ArrayList<Future<List<String>>>();

        for(int i=0;i<16;i++){
            tasks.add(service.submit(new ScanUID("node" + i,pid,startKey,endKey)));
        }

        for(Future<List<String>> uids : tasks){
            try{
                for(String str : uids.get()){
                    System.out.println(str);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

    }

}

class ScanUID implements Callable<List<String>>{

    String node;
    String tableName;
    String startKey;
    String endKey;

    public ScanUID(String node,String tableName,String startKey,String endKey){
        this.node = node;
        this.tableName = tableName;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    @Override
    public List<String> call() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));
        scan.setMaxVersions(1); //只需要一个version
        scan.setCaching(10000);
        scan.setFilter(new KeyOnlyFilter());

        HTable table = new HTable(conf,"deu_" + tableName);
        ResultScanner scanner = table.getScanner(scan);
        List<String> results = new ArrayList<String>();
        for(Result r : scanner){
            long uid = Utils.transformerUID(Bytes.tail(r.getRow(), 5));
            results.add(uid + "");
        }
        return results;
    }
}

