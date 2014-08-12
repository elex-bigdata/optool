package com.elex.ba.mapper;

import com.elex.ba.util.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-6-6
 * Time: 下午4:01
 */
public class RegistUserMapper extends Mapper<Text,Text,Text,Text>{

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if(fileName.endsWith("register_time.log")){
            context.write(key,new Text(Constants.node_prefix +value.toString()));
        }else{
            //将MYSQL的uid做MD5转换 与HBASE的UID 格式一致
            try{
                long innerUID = Long.parseLong(key.toString());
                long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(innerUID);
                context.write(new Text(String.valueOf(samplingUid)),new Text(Constants.idmap_prefix+ value.toString()));
            }catch(Exception e){

            }
        }
    }
}
