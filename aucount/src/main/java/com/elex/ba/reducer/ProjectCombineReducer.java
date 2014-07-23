package com.elex.ba.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class ProjectCombineReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iter =  values.iterator();

        int count = 0;
        while(iter.hasNext()){
            count += iter.next().get();
        }
        context.write(key,new IntWritable(count));
    }
}
