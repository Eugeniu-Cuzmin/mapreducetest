package com.mapreader;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

public class SumReducer implements Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable intWritable = new IntWritable();

    public void reduce(Text key,
                       Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> outputCollector,
                       Reporter reporter) throws IOException {
        int wordCount = 0;
        IntWritable words = new IntWritable();
        /*for (IntWritable value : values) {
            wordCount += value.get();
        }*/
        while(values.hasNext()){
            wordCount += values.next().get();
        }
        intWritable.set(wordCount);
        outputCollector.collect(key, intWritable);
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}