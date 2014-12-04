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
public class UIDCombineReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> months = new HashSet<String>();
        String orguid = "";
        for(Text val : values){
            String data = val.toString();
            if(data.startsWith(Constants.mau_month_prefix)){
                months.add(data.substring(Constants.mau_month_prefix.length()));
            }else{
                orguid = data;
            }
        }

        for(String month : months){
            context.write(new Text(orguid),new Text(month));
        }
    }
}
