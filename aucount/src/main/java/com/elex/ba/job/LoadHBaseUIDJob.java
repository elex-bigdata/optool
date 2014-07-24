package com.elex.ba.job;

import com.elex.ba.mapper.LoadHBaseUIDMapper;
import com.elex.ba.reducer.LoadHBaseUIDReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * 将指定项目的HBase visit的UID提取出来，输出格式为 UID,MONTH
 * Date: 14-7-23
 * Time: 下午4:55
 */
public class LoadHBaseUIDJob implements Callable<Integer> {

    private Set<String> projects;
    private String node;


    public LoadHBaseUIDJob(String node, Set<String> projects){
        this.projects = projects;
        this.node = node;
    }

    public int run(String project) throws IOException, ClassNotFoundException, InterruptedException {
        byte[] table = Bytes.toBytes("deu_" + project);
        Path outputpath = new Path("/user/hadoop/offlinetest/node/" + node + "/" + project);
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("20140601visit"));
        scan.setStopRow(Bytes.toBytes("20140602visit"));
        scan.setMaxVersions(1);
        scan.setCaching(4000);
        scan.setFilter(new KeyOnlyFilter());

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx512m") ;

        Job job = new Job(conf,node + "_" + project);
        job.setJarByClass(LoadHBaseUIDJob.class);
        TableMapReduceUtil.initTableMapperJob(table, scan, LoadHBaseUIDMapper.class, Text.class, Text.class, job);
        job.setCombinerClass(LoadHBaseUIDReducer.class);
        job.setReducerClass(LoadHBaseUIDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);


        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public Integer call() {
        for(String p : projects){

            try {
                if(run(p) == 0){
                    System.out.println(node + " " + p + " success");
                }else{
                    System.out.println(node + " " + p + " fail");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 1;
    }
}