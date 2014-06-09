package com.elex.bigdata.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午4:01
 */
public class QuartorMapper extends TableMapper<Text,IntWritable> {

//    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//    private Map<String,String> quartors = new LinkedHashMap<String,String>();

    private IntWritable count = new IntWritable(1);
/*    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String[] years = new String[]{"2011","2012","2013"};
        for(String y : years){
            for(int i=1;i<=12;i++){
                String q = "04";
                if(i< 4){
                    q = "01";
                }else if(i < 7){
                    q = "02";
                }else if(i < 10){
                    q = "03";
                }
                String m = String.valueOf(i);
                if(i <=9){
                    m = "0" + i;
                }
                quartors.put(y+m , y + q);
            }
        }
    }*/

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        try{
            String m = Bytes.toStringBinary(Bytes.head(key.get(),6));
            //String q = quartors.get(m);
            /*if(q != null){
                String uid = Bytes.toStringBinary(Bytes.tail(key.get(),5));
                context.write(new Text(q + uid),count);
            }*/
            String uid = Bytes.toStringBinary(Bytes.tail(key.get(),5));
            context.write(new Text(m + uid),count);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
