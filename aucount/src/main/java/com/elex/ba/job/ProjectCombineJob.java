package com.elex.ba.job;

import com.elex.ba.mapper.ProjectCombineMapper;
import com.elex.ba.reducer.ProjectCombineReducer;
import com.elex.bigdata.mapper.CombineMapper;
import com.elex.bigdata.reducer.CombineReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Author: liqiang
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class ProjectCombineJob {

    private Map<String,Set<String>> projects;


    public ProjectCombineJob(Map<String,Set<String>>  projects){
        this.projects = projects;
    }

    public int run(String project, Set<String> pids) throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path("/user/hadoop/offline/combine/" + project);


        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx512m") ;

        Job job = new Job(conf,"pcombine_" + project);
        job.setJarByClass(ProjectCombineJob.class);
        job.setMapperClass(ProjectCombineMapper.class);
        job.setCombinerClass(ProjectCombineReducer.class);
        job.setReducerClass(ProjectCombineReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        conf.set("pid",project);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        for(String pid : pids){
            Path p = new Path("/user/hadoop/offline/project/"+pid);
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
        for(String p : projects.keySet()){

            try {
                if(run(p,projects.get(p)) == 0){
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
