package org.hoshi.tut.hadoop.anagram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;

import java.util.LinkedList;

public class AnagramUnitTest {
    public static final Text TEST_INPUT_1 = new Text("Elvis still lives.");
    public static final Text TEST_INPUT_2 = new Text("Live not on evil deed, live not on evil.");

    @Test
    public void testMapper() throws Exception {
        final MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<>();

        mapDriver.setMapper(new AnagramMapper());

        mapDriver.addInput(new LongWritable(0),  TEST_INPUT_1);
        mapDriver.addInput(new LongWritable(13), TEST_INPUT_2);

        mapDriver.addOutput(new Text("[e, i, l, s, v]"), new Text("elvis"));
        mapDriver.addOutput(new Text("[i, l, l, s, t]"), new Text("still"));
        mapDriver.addOutput(new Text("[e, i, l, s, v]"), new Text("lives"));

        mapDriver.addOutput(new Text("[e, i, l, v]"), new Text("live"));
        mapDriver.addOutput(new Text("[n, o, t]"), new Text("not"));
        mapDriver.addOutput(new Text("[n, o]"), new Text("on"));
        mapDriver.addOutput(new Text("[e, i, l, v]"), new Text("evil"));
        mapDriver.addOutput(new Text("[d, d, e, e]"), new Text("deed"));
        mapDriver.addOutput(new Text("[e, i, l, v]"), new Text("live"));
        mapDriver.addOutput(new Text("[n, o, t]"), new Text("not"));
        mapDriver.addOutput(new Text("[n, o]"), new Text("on"));
        mapDriver.addOutput(new Text("[e, i, l, v]"), new Text("evil"));

        mapDriver.runTest(false);
    }

    @Test
    public void testReducer() throws Exception {
        final ReduceDriver<Text, Text, Text, Text> reduceDriver = new ReduceDriver<>();

        reduceDriver.setReducer(new AnagramReducer());

        final Text key1 = new Text("[e, i, l, s, v]");
        reduceDriver.addInput(key1, new LinkedList<Text>() {{
                                          add(new Text("elvis"));
                                          add(new Text("lives"));
                                    }});

        final Text key2 = new Text("[e, i, l, v]");
        reduceDriver.addInput(key2, new LinkedList<Text>() {{
                                          add(new Text("live"));
                                          add(new Text("evil"));
                                          add(new Text("live"));
                                          add(new Text("evil"));
                                    }});

        reduceDriver.addOutput(key1, new Text("elvis, lives"));
        reduceDriver.addOutput(key2, new Text("evil, live"));

        reduceDriver.runTest(false);
    }

    @Test
    public void testMapReduce() throws Exception {
        final MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver = new MapReduceDriver<>();

        mapReduceDriver.setMapper(new AnagramMapper());
        mapReduceDriver.setReducer(new AnagramReducer());

        mapReduceDriver.addInput(new LongWritable(0),  TEST_INPUT_1);
        mapReduceDriver.addInput(new LongWritable(13), TEST_INPUT_2);

        mapReduceDriver.addOutput(new Text("[e, i, l, s, v]"), new Text("elvis, lives"));
        mapReduceDriver.addOutput(new Text("[e, i, l, v]"),    new Text("evil, live"));

        mapReduceDriver.runTest(false);
    }
}