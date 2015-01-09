package org.hoshi.tut.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.testng.annotations.Test;

public class WordCountCaseSensitiveUnitTest {
    @Test
    public void testCaseSensitiveMapper() throws Exception {
        final MapDriver<LongWritable, Text, Text, IntWritable> mapDriver =
                new MapDriver<>();

        mapDriver.setMapper(new WordCountCaseSensitiveMapper());

        // set case-sensitive parameter to true
        mapDriver.getConfiguration().set(
                WordCountCaseSensitiveMapper.CONF_CASE_SENSITIVE, "true");

        mapDriver.addInput(new LongWritable(0), WordCountUnitTest.TEST_INPUT_1);

        mapDriver.addOutput(new Text("In"),     new IntWritable(1));
        mapDriver.addOutput(new Text("a"),      new IntWritable(1));
        mapDriver.addOutput(new Text("hole"),   new IntWritable(1));
        mapDriver.addOutput(new Text("in"),     new IntWritable(1));
        mapDriver.addOutput(new Text("the"),    new IntWritable(1));
        mapDriver.addOutput(new Text("ground"), new IntWritable(1));
        mapDriver.addOutput(new Text("there"),  new IntWritable(1));
        mapDriver.addOutput(new Text("lived"),  new IntWritable(1));
        mapDriver.addOutput(new Text("a"),      new IntWritable(1));
        mapDriver.addOutput(new Text("hobbit"), new IntWritable(1));

        mapDriver.runTest(false); // <= ignore order of emitted keys
    }

    @Test
    public void testCaseInSensitiveMapper() throws Exception {
        final MapDriver<LongWritable, Text, Text, IntWritable> mapDriver =
                new MapDriver<>();

        mapDriver.setMapper(new WordCountCaseSensitiveMapper());

        // set case-sensitive parameter to false
        mapDriver.getConfiguration().set(
                WordCountCaseSensitiveMapper.CONF_CASE_SENSITIVE, "false");

        mapDriver.addInput(new LongWritable(0), WordCountUnitTest.TEST_INPUT_1);

        mapDriver.addOutput(new Text("in"),     new IntWritable(1));
        mapDriver.addOutput(new Text("a"),      new IntWritable(1));
        mapDriver.addOutput(new Text("hole"),   new IntWritable(1));
        mapDriver.addOutput(new Text("in"),     new IntWritable(1));
        mapDriver.addOutput(new Text("the"),    new IntWritable(1));
        mapDriver.addOutput(new Text("ground"), new IntWritable(1));
        mapDriver.addOutput(new Text("there"),  new IntWritable(1));
        mapDriver.addOutput(new Text("lived"),  new IntWritable(1));
        mapDriver.addOutput(new Text("a"),      new IntWritable(1));
        mapDriver.addOutput(new Text("hobbit"), new IntWritable(1));

        mapDriver.runTest(false); // <= ignore order of emitted keys
    }

    @Test
    public void testCaseSensitiveMapReduce() throws Exception {
        final MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
                new MapReduceDriver<>();

        mapReduceDriver.setMapper(new WordCountCaseSensitiveMapper());
        mapReduceDriver.setReducer(new WordCountReducerV2());

        // set case-sensitive parameter to true
        mapReduceDriver.getConfiguration().set(
                WordCountCaseSensitiveMapper.CONF_CASE_SENSITIVE, "true");

        mapReduceDriver.addInput(new LongWritable(0), WordCountUnitTest.TEST_INPUT_1);

        mapReduceDriver.addOutput(new Text("In"),     new IntWritable(1));
        mapReduceDriver.addOutput(new Text("a"),      new IntWritable(2));
        mapReduceDriver.addOutput(new Text("hole"),   new IntWritable(1));
        mapReduceDriver.addOutput(new Text("in"),     new IntWritable(1));
        mapReduceDriver.addOutput(new Text("the"),    new IntWritable(1));
        mapReduceDriver.addOutput(new Text("ground"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("there"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("lived"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("hobbit"), new IntWritable(1));

        mapReduceDriver.runTest(false); // <= ignore order of emitted keys
    }

    @Test
    public void testCaseInSensitiveMapReduce() throws Exception {
        final MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
                new MapReduceDriver<>();

        mapReduceDriver.setMapper(new WordCountCaseSensitiveMapper());
        mapReduceDriver.setReducer(new WordCountReducerV2());

        // set case-sensitive parameter to false
        mapReduceDriver.getConfiguration().set(
                WordCountCaseSensitiveMapper.CONF_CASE_SENSITIVE, "false");

        mapReduceDriver.addInput(new LongWritable(0), WordCountUnitTest.TEST_INPUT_1);

        mapReduceDriver.addOutput(new Text("in"),     new IntWritable(2));
        mapReduceDriver.addOutput(new Text("a"),      new IntWritable(2));
        mapReduceDriver.addOutput(new Text("hole"),   new IntWritable(1));
        mapReduceDriver.addOutput(new Text("the"),    new IntWritable(1));
        mapReduceDriver.addOutput(new Text("ground"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("there"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("lived"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("hobbit"), new IntWritable(1));

        mapReduceDriver.runTest(false); // <= ignore order of emitted keys
    }
}