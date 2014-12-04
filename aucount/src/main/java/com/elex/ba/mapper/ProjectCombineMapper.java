package com.elex.ba.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午4:01
 */
public class ProjectCombineMapper extends Mapper<Text,Text,Text,Text>{

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //key = uid value = month
//        String v = value.toString() + key.toString();

        //key linenum , value line(uid)
        context.write(key,value);
    }
}
