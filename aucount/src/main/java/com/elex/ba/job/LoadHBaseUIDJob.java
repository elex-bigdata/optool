package com.elex.ba.job;

import com.elex.ba.mapper.LoadHBaseUIDMapper;
import com.elex.ba.reducer.LoadHBaseUIDReducer;
import com.elex.ba.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * 将指定项目的HBase visit的UID提取出来，
 * 为了兼容下一步MYSQL IDMAP的输入，输出为 UID\t''
 * Date: 14-7-23
 * Time: 下午4:55
 */
public class LoadHBaseUIDJob implements Callable<Integer> {

    private String pid;
    private String node;
    private String date;


    public LoadHBaseUIDJob(String date,String node, String pid){
        this.date = date;
        this.pid = pid;
        this.node = node;
    }

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        byte[] table = Bytes.toBytes("deu_" + pid);
        Path outputpath = new Path(Utils.getHBaseUIDPath(date, node, pid));
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("20140625visit"));
        scan.setStopRow(Bytes.toBytes("20140724visiu"));
        scan.setMaxVersions(1); //只需要一个version
        scan.setCaching(10000);
        scan.setFilter(new KeyOnlyFilter());
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*visit.*")));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", node);
        conf.set("hbase.zookeeper.property.clientPort", "3181");
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts", "-Xmx512m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);

        Job job = new Job(conf,node + "_" + pid);
        job.setJarByClass(LoadHBaseUIDJob.class);
        TableMapReduceUtil.initTableMapperJob(table, scan, LoadHBaseUIDMapper.class, Text.class, Text.class, job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setCombinerClass(LoadHBaseUIDReducer.class);
        job.setReducerClass(LoadHBaseUIDReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(3);

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

        try {
            if(run() == 0){
                System.out.println(node + " " + pid + " success");
            }else{
                System.out.println(node + " " + pid + " fail");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}