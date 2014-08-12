package com.elex.ba.reducer;

import com.elex.ba.util.Constants;
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
public class RegistUserReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String registTime = "";
        String orguid = "";
        for(Text val : values){
            String data = val.toString();
            if(data.startsWith(Constants.node_prefix)){
                registTime = data.substring(Constants.node_prefix.length());
            }else{
                orguid = data.substring(Constants.idmap_prefix.length());
            }
        }

        if(registTime.trim().length() > 0 && orguid.trim().length() > 0){
            context.write(new Text(orguid),new Text(registTime));
        }
    }
}
