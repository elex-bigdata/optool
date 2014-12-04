package com.elex.ba.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午4:01
 */
public class ProjectCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,count);
    }
}
