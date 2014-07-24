package com.elex.ba.job;

import com.elex.ba.mapper.UIDCombineMapper;
import com.elex.ba.reducer.UIDCombineReducer;
import com.elex.ba.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Author: liqiang
 * 按小项目合并并转换16个HBase节点UID
 * Date: 14-6-7
 * Time: 上午10:35
 */
public class UIDCombineJob implements Callable<Integer> {

    private String project; //小项目名
    private String date;

    public UIDCombineJob(String date, String project){
        this.date = date;
        this.project = project;
    }

    public int run(String project) throws IOException, ClassNotFoundException, InterruptedException {
        Path outputpath = new Path(Utils.getUIDCombinePath(date,project));

        Configuration conf = new Configuration();
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts","-Xmx512m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);

        Job job = new Job(conf,"combine_" + project);
        job.setJarByClass(UIDCombineJob.class);
        job.setMapperClass(UIDCombineMapper.class);
        job.setReducerClass(UIDCombineReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(5);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job,outputpath);
        for(int i =0;i<16; i++){
            Path p = new Path(Utils.getHBaseUIDPath(date,"node" + i,project));
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

        try {
            if(run(project) == 0){
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
