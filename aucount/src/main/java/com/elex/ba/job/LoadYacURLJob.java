package com.elex.ba.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;


public class LoadYacURLJob {



    public int run() throws IOException, ClassNotFoundException, InterruptedException, ParseException {
        byte[] table = Bytes.toBytes("yac_user_action");
        Path outputpath = new Path("/user/hadoop/yac/url");
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("1"));
        scan.setStopRow(Bytes.toBytes("2"));
        scan.setMaxVersions(1); //只需要一个version
        scan.setCaching(10000);

        scan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("url"));
        scan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("nt"));

        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("ua"), Bytes.toBytes("nt"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("br")) ));
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("ua"), Bytes.toBytes("nt"), CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("es")) ));

        Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
        scan.setFilter(filterList);

        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "dmnode3,dmnode4,dmnode5");
//        conf.set("hbase.zookeeper.property.clientPort", "3181");
        conf.set("mapred.child.java.opts", "-Xmx1024m");
        conf.set("mapred.map.child.java.opts","-Xmx512m") ;
        conf.set("mapred.reduce.child.java.opts", "-Xmx512m") ;
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setClass("mapred.map.output.compression.codec", Lz4Codec.class, CompressionCodec.class);

        Job job = new Job(conf, "YAC");
        job.setJarByClass(LoadYacURLJob.class);
        TableMapReduceUtil.initTableMapperJob(table, scan, LoadYacNationURLMapper.class, Text.class, Text.class, job);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(LoadYacNationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(40);

        MultipleOutputs.addNamedOutput(job, "br", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "es", TextOutputFormat.class, Text.class, Text.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputpath)) {
            fs.delete(outputpath, true);
        }

        FileOutputFormat.setOutputPath(job, outputpath);

        job.waitForCompletion(true);

        if (job.isSuccessful()) {
            return 0;
        } else {
            return -1;
        }
    }

}

class LoadYacNationURLMapper extends TableMapper<Text,NullWritable> {

    byte[] family = Bytes.toBytes("ua");
    byte[] urlCol = Bytes.toBytes("url");
    byte[] nationCol = Bytes.toBytes("nt");

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        try{
            byte[] rowKey = key.get();
            String uid = Bytes.toString(Bytes.tail(rowKey, rowKey.length - 9));

            String url = Bytes.toString(value.getColumn(family, urlCol).get(0).getValue());
            String nation = Bytes.toString(value.getColumn(family, nationCol).get(0).getValue());

            context.write(new Text(nation + "\t" + uid + "\t" + url), NullWritable.get());
        } catch (Exception e) {

        }

    }
}

class LoadYacNationReducer extends Reducer<Text,NullWritable,Text,Text> {

    private MultipleOutputs<Text,Text> mos;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        mos=new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String[] kv = key.toString().split("\t");
        if(kv.length == 3){
            mos.write(kv[0],new Text(kv[1]),new Text(kv[2]));
        }
    }

    protected void cleanup(Context context) throws IOException,InterruptedException {
        mos.close();
    }

}
