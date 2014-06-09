package com.elex.bigdata.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class QuartorCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable value = new IntWritable(1);
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
