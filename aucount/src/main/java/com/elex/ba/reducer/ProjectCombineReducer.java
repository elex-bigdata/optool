package com.elex.ba.reducer;

import com.elex.ba.util.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class ProjectCombineReducer extends Reducer<Text,Text,Text,NullWritable> {

    private MultipleOutputs<Text,Text> mos;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        mos=new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> days = new HashSet<String>();
        for(Text day : values){
            days.add(day.toString());
        }
        for(String day : days){
            mos.write(day,key,NullWritable.get(),day);
        }
    }

    protected void cleanup(Context context) throws IOException,InterruptedException {
        mos.close();
    }
}
