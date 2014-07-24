package com.elex.ba.reducer;

import com.elex.ba.util.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午6:51
 */
public class UIDCombineReducer extends Reducer<Text,Text,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val : values){
            String data = val.toString();
            if(data.length() != 0){
                context.write(new Text(data),NullWritable.get());
                break;
            }
        }

   /*     for(String month : months){
            context.write(new Text(orguid),new Text(month));
        }*/
    }
}
