package com.elex.ba.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: liqiang
 * Date: 14-12-9
 * Time: 下午2:57
 */
public class CleanTable {

    public static void main(String[] args) throws IOException {

        List<String> projects = new ArrayList<String>();
        projects.add("sof-webcake");
        projects.add("sof-yontoo");
        projects.add("sof-yandex");
        projects.add("v9-ssvyk");
        projects.add("sof-um");
        projects.add("sof-wd");
        projects.add("v9-rdm");
        projects.add("v9-mhc");
        projects.add("v9-vtnet");
        projects.add("sof-lol");
        projects.add("sof-addlyrics");
        projects.add("v9-nur");
        projects.add("sof-iminent");
        projects.add("sof-pcf");
        projects.add("sof-pmillion");
        projects.add("sof-pcsuzk");
        projects.add("sof-qtp");
        projects.add("iobit");
        projects.add("v9-mtix");
        projects.add("v9-ttinew");
        projects.add("soft-twitter-assist");
        projects.add("sof-easydl");

        for(int i=0;i<16;i++){
            for(String pid : projects){
                deleteTable("deu_" + pid, "node" + i);
            }
        }
    }

    public static void deleteTable(String tableName,String node) throws IOException {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", node);
            conf.set("hbase.zookeeper.property.clientPort", "3181");
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.isTableAvailable(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("Table "+ node + " " +tableName + " deleted sucessfully!");
            } else
                System.out.println("Table " + node + " " + tableName + " not exist.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
}
