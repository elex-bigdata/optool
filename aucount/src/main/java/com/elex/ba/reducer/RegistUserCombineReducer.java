package com.elex.ba.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class RegistUserCombineReducer extends Reducer<Text,Text,Text,NullWritable> {

    private String date;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        date = context.getConfiguration().get("regist.date");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String max = "99999900000000";
        for(Text c : values){
            max = max.compareTo(c.toString()) > 0 ? c.toString() : max;
        }
        if(max.startsWith(date)){
            context.write(key, NullWritable.get());
            context.getCounter("regist","user").increment(1);
        }

        if(max.startsWith("201408")){
            context.getCounter("regist",max.substring(0,8)).increment(1);
        }

    }
}
