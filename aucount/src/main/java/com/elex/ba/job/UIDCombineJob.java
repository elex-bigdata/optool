package com.elex.ba.job;

import com.elex.ba.mapper.UIDCombineMapper;
import com.elex.ba.reducer.UIDCombineReducer;
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

/**
 * Author: liqiang
 * join uid，并去重
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class UIDCombineJob {

    private Set<String> projects;


    public UIDCombineJob(Set<String> projects){
        this.projects = projects;
    }

    public int run(String project) throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path("/user/hadoop/offline/project/" + project);


        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx512m") ;

        Job job = new Job(conf,"combine_" + project);
        job.setJarByClass(UIDCombineJob.class);
        job.setMapperClass(UIDCombineMapper.class);
        job.setReducerClass(UIDCombineReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        conf.set("pid",project);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        for(int i =0;i<16; i++){
            String nodename = "node" + i;
            Path p = new Path("/user/hadoop/offline/node/" + nodename + "/" + project);
            if(fs.exists(p)){
                FileInputFormat.addInputPath(job,p);
            }
        }

        String idmapPath = "/user/hadoop/mysqlidmap/vf_" + project;
        FileInputFormat.addInputPath(job, new Path(idmapPath));

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
