package com.elex.ba.inputformat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午5:53
 */
public class TextCombineInputformat extends CombineFileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit,
                    TaskAttemptContext taskAttemptContext) throws IOException {
        return new CombineFileRecordReader((CombineFileSplit) inputSplit, taskAttemptContext,
                (Class)LineRecordReader.class);
    }
}
