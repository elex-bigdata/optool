package com.elex.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-4-3
 * Time: 下午6:06
 */
public class HBaseUtil {

    private static HTablePool pool;

    public static void init(){
        Configuration conf = HBaseConfiguration.create();
        pool = new HTablePool(conf, 100);
    }

    public static HTableInterface getHTable(String tableName) throws IOException {
        try {
            return pool.getTable(tableName);
        } catch (Exception e) {
            throw new IOException("Cannot get htable from hbase(" + tableName + ").", e);
        }
    }

}
