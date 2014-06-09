package com.elex.bigdata.job;

import com.elex.bigdata.mapper.CombineMapper;
import com.elex.bigdata.reducer.CombineReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class CombinerJob{

    private List<String> projects;


    public CombinerJob( List<String> projects){
        this.projects = projects;
    }

    public int run(String project) throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path("/user/hadoop/quartorcombine/" + project);


        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx512m") ;

        Job job = new Job(conf,"combine_" + project);
        job.setJarByClass(CombinerJob.class);
        job.setMapperClass(CombineMapper.class);
        job.setCombinerClass(CombineReducer.class);
        job.setReducerClass(CombineReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        for(int i =0;i<16; i++){
            String nodename = "node" + i;
            Path p = new Path("/user/hadoop/quartorcount/" + nodename + "/" + project);
            if(fs.exists(p)){
                FileInputFormat.addInputPath(job,p);
            }
        }

        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            return 0;
        } else {
            return -1;
        }
    }

    public Integer call()  {
        for(String p : projects){

            try {
                if(run(p) == 0){
                    System.out.println(" " + p + " success");
                }else{
                    System.out.println(" " + p + " fail");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 1;
    }
}
