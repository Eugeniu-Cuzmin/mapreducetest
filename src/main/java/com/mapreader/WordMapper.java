package com.mapreader;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

public class WordMapper implements Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable intWritable = new IntWritable(1);
    Text text = new Text();

    public void map(LongWritable key,
                    Text value,
                    OutputCollector<Text, IntWritable> outputCollector,
                    Reporter reporter) throws IOException {

        String line = value.toString();
        for (String word : line.split("\\W+")) {
            if (word.length() > 0) {
                text.set(word);
                outputCollector.collect(text, intWritable);
            }
        }
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}