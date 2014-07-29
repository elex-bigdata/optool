package com.elex.ba.reducer;

import com.elex.ba.util.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午5:17
 */
public class LoadHBaseUIDReducer extends Reducer<Text,Text,Text,Text> {

//    private Text empty = new Text("");
//    private MultipleOutputs<Text,Text> mos;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
//        mos=new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> days = new HashSet<String>();
        for(Text day : values){
            days.add(day.toString());
        }
        for(String day : days){
            context.write(key,new Text(Constants.mau_month_prefix + day));
        }

    }

/*    protected void cleanup(Context context) throws IOException,InterruptedException {
        mos.close();
    }*/
}