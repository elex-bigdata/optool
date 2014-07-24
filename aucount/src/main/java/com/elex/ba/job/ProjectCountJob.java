package com.elex.ba.job;

import com.elex.ba.inputformat.TextCombineInputformat;
import com.elex.ba.mapper.ProjectCountMapper;
import com.elex.ba.reducer.ProjectCountReducer;
import com.elex.ba.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * 统计
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class ProjectCountJob implements Callable<Integer> {

    private String date;
    private String project ;

    public ProjectCountJob(String date, String project){
        this.date = date;
        this.project = project;
    }

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path(Utils.getProjectCountPath(date,project));

        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx1024m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx1024m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);

        Job job = new Job(conf,"pcount_" + project);
        job.setJarByClass(ProjectCountJob.class);
        job.setMapperClass(ProjectCountMapper.class);
        job.setCombinerClass(ProjectCountReducer.class);
        job.setReducerClass(ProjectCountReducer.class);
        job.setInputFormatClass(TextCombineInputformat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        Path p = new Path(Utils.getProjectCombinePath(date,project));

        if(fs.exists(p)){
            FileInputFormat.addInputPath(job,p);
        }

        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            return 0;
        } else {
            return -1;
        }
    }

    public Integer call()  {
        try {
            if(run() == 0){
                System.out.println(" " + project + " success");
            }else{
                System.out.println(" " + project + " fail");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}
