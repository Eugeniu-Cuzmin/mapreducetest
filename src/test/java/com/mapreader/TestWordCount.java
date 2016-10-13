package com.mapreader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordMapper mapper = new WordMapper();
        SumReducer reducer = new SumReducer();
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapReduce() {
        System.setProperty("hadoop.home.dir", "C:\\WorkSpace\\");
        mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

}
