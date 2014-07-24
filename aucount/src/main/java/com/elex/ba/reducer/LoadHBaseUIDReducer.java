package com.elex.ba.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午5:17
 */
public class LoadHBaseUIDReducer extends Reducer<Text,NullWritable,Text,Text> {

    private Text empty = new Text("");

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        /*Set<String> months = new HashSet<String>();
        for(Text month : values){
            months.add(month.toString());
        }
        for(String month : months){
            context.write(key,new Text(month));
        }*/
        context.write(key,empty);
    }
}