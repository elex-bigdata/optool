package com.elex.ba.job;

import com.elex.ba.inputformat.TextCombineInputformat;
import com.elex.ba.mapper.ProjectCombineMapper;
import com.elex.ba.reducer.ProjectCombineReducer;
import com.elex.ba.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * 所有指定项目数据合并起来
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class ProjectCombineJob implements Callable<Integer> {

    private String project ; //internet大分类
    private Set<String> pids; //小项目名
    private String date;

    public ProjectCombineJob(String date, String project, Set<String> pids){
        this.date = date;
        this.project = project;
        this.pids = pids;
    }

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path(Utils.getProjectCombinePath(date,project));


        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx1024m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx1024m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);

        Job job = new Job(conf,"pcombine_" + project);
        job.setJarByClass(ProjectCombineJob.class);
        job.setMapperClass(ProjectCombineMapper.class);
        job.setCombinerClass(ProjectCombineReducer.class);
        job.setReducerClass(ProjectCombineReducer.class);
        job.setInputFormatClass(TextCombineInputformat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(10);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        for(String pid : pids){
            Path p = new Path(Utils.getUIDCombinePath(date,pid));
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
