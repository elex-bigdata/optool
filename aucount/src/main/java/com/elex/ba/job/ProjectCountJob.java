package com.elex.ba.job;

import com.elex.ba.mapper.ProjectCombineMapper;
import com.elex.ba.mapper.ProjectCountMapper;
import com.elex.ba.reducer.ProjectCombineReducer;
import com.elex.ba.reducer.ProjectCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class ProjectCountJob implements Callable<Integer> {

    private String project ;


    public ProjectCountJob(String project){
        this.project = project;
    }

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path("/user/hadoop/offline/count/" + project);


        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx1024m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx1024m") ;

        Job job = new Job(conf,"pcount_" + project);
        job.setJarByClass(ProjectCountJob.class);
        job.setMapperClass(ProjectCountMapper.class);
        job.setCombinerClass(ProjectCountReducer.class);
        job.setReducerClass(ProjectCountReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        Path p = new Path("/user/hadoop/offline/combine/"+project);
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
