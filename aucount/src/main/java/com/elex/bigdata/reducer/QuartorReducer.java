package com.elex.bigdata.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class QuartorReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
    private NullWritable value = NullWritable.get();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String qu = key.toString();
        String q = qu.substring(0,6);
        context.write(new Text(q),value);
    }
}
