package com.elex.ba.job;

import com.elex.ba.mapper.ProjectCombineMapper;
import com.elex.ba.reducer.ProjectCombineReducer;
import com.elex.ba.reducer.RegistUserCombineReducer;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * 合并internet大项目下的所有小项目UID
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class RegistUserCombineJob implements Callable<Integer> {

    private String project ; //internet大分类
    private Set<String> pids; //小项目名
    private String date;

    public RegistUserCombineJob(String date, String project, Set<String> pids){
        this.date = date;
        this.project = project;
        this.pids = pids;
    }

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path(Utils.getRegistCombinePath(date, project));


        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx1024m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx1024m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);
        conf.set("regist.date",date);

        Job job = new Job(conf,"RegistUserCombine_" +date+ project);
        job.setJarByClass(RegistUserCombineJob.class);
        job.setMapperClass(ProjectCombineMapper.class);
        job.setReducerClass(RegistUserCombineReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(6);


        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }
        FileOutputFormat.setOutputPath(job, outputpath);

        FileOutputFormat.setOutputPath(job,outputpath);
        List<Path> inputPaths = new ArrayList<Path>();
        for(String pid : pids){
            Path p = new Path(Utils.getRegistPath(pid));
            if(fs.exists(p)){
                FileInputFormat.addInputPath(job,p);
                inputPaths.add(p);
            }
        }

        Path p = new Path("/user/hadoop/liqiang/all_uid.log");
        FileInputFormat.addInputPath(job,p);

        job.waitForCompletion(true);

        if (job.isSuccessful()) {
/*            for(Path p : inputPaths){
//                fs.delete(p, true);
            }*/
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
