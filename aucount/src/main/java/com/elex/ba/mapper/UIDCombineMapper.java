package com.elex.ba.mapper;

import com.elex.ba.util.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午4:01
 */
public class UIDCombineMapper extends Mapper<Text,Text,Text,Text>{

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        if(value.toString().startsWith(Constants.mau_month_prefix)){
            context.write(key,value);
        }else{
            //将MYSQL的uid做MD5转换 与HBASE的UID 格式一致
            long innerUID = Long.parseLong(key.toString());
            long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(innerUID);
            context.write(new Text(String.valueOf(samplingUid)),value);
        }
    }
}
