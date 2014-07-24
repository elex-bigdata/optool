package com.elex.ba.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class ProjectCombineReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //key= month value=uid

        for(Text month : values){
            context.write(key, month); 
        }

    }
}
