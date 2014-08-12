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
    private static String max = "99999900000000";

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        date = context.getConfiguration().get("regist.date");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String min = "99999900000000";
        boolean installer = false;
        for(Text c : values){
            if(c == null || c.toString().length() == 0){
                installer = true;
                continue;
            }
            min = min.compareTo(c.toString()) > 0 ? c.toString() : min;
        }
        if(installer && min.startsWith(date)){
            context.write(key, NullWritable.get());
            context.getCounter("regist","user").increment(1);
        }

/*        if(min.startsWith("201408")){
            context.getCounter("regist",min.substring(0,8)).increment(1);
        }*/

        if(installer){
            if(!max.equals(min)){
                context.getCounter("regist","miss").increment(1);
            }else{
                context.getCounter("regist",min.substring(0,8)).increment(1);
            }
        }

    }
}
