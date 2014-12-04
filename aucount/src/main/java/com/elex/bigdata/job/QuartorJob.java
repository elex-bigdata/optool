package com.elex.bigdata.job;

import com.elex.bigdata.mapper.QuartorMapper;
import com.elex.bigdata.reducer.QuartorCombiner;
import com.elex.bigdata.reducer.QuartorReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class QuartorJob implements Callable<Integer> {

    private List<String> projects;
    private String node;


    public QuartorJob(String node, List<String> projects){
        this.projects = projects;
        this.node = node;
    }

    public int run(String project) throws IOException, ClassNotFoundException, InterruptedException {
        byte[] table = Bytes.toBytes("deu_" + project);
        Path outputpath = new Path("/user/hadoop/quartorcount/" + node + "/" + project);
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("20130101visit"));
        scan.setStopRow(Bytes.toBytes("20140100visit"));
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
        job.setJarByClass(QuartorJob.class);
        TableMapReduceUtil.initTableMapperJob(table,scan, QuartorMapper.class,Text.class, IntWritable.class,job);
        job.setCombinerClass(QuartorCombiner.class);
        job.setReducerClass(QuartorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);


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
