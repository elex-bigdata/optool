package com.elex.ba.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class ProjectCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable c : values){
            count += c.get();
        }
        context.write(key, new IntWritable(count));
    }
}
