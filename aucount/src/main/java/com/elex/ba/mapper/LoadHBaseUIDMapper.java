package com.elex.ba.mapper;

import com.elex.ba.util.Constants;
import com.elex.ba.util.Utils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午5:03
 */
public class LoadHBaseUIDMapper extends TableMapper<Text,Text> {

    private Text empty = new Text("");
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        try{
            long uid = Utils.transformerUID(Bytes.tail(key.get(),5)); //将HBASE uid转换为 samplingUID ，将与MYSQL ID 做关联
            /*String m = Bytes.toStringBinary(Bytes.head(key.get(), 6)); //月份
            context.write(new LongWritable(uid),new Text(Constants.mau_month_prefix + m));*/
            String day = Bytes.toStringBinary(Bytes.head(key.get(), 8));
            context.write(new Text(day + uid), empty);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}