package com.elex.ba.job;

import com.elex.ba.mapper.RegistUserMapper;
import com.elex.ba.reducer.RegistUserReducer;
import com.elex.ba.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * Date: 14-8-12
 * Time: 上午10:57
 */
public class RegistUserJob implements Callable<Integer> {

    private String date;
    private String project ;

    public RegistUserJob(String date, String project){
        this.date = date;
        this.project = project;
    }

    public int run() throws IOException, ClassNotFoundException, InterruptedException, ParseException {
        Path outputpath = new Path(Utils.getRegistPath(project));

        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx1024m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx1024m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);

        Job job = new Job(conf,"regist_" +date + "_" + project);
        job.setJarByClass(RegistUserJob.class);
        job.setMapperClass(RegistUserMapper.class);
        job.setReducerClass(RegistUserReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);

        //user/hadoop/mysql/22apple/node3_register_time.log
        for(int i=0;i<16;i++){
            Path p = new Path(Utils.getMysqlAttrPath("node" + i,project,"register_time") );
            if(fs.exists(p)){
                FileInputFormat.addInputPath(job, p);
            }
        } 
        Path p = new Path("/user/hadoop/mysqlidmap/vf_"+project);
        FileInputFormat.addInputPath(job, p);

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