/**
 * The MIT License (MIT)
 *
 * Copyright (C) 2014 Luka Obradovic.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.hoshi.tut.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountUnitTest {
    public static final Text TEST_INPUT_1 = new Text("In a hole in the ground there lived a hobbit.");
    public static final Text TEST_INPUT_2 = new Text("The world is indeed full of peril and in it there are many dark places.");

    @Test
    public void testMapper() throws Exception {
        final MapDriver<LongWritable, Text, Text, IntWritable> mapDriver =
                new MapDriver<>();

        mapDriver.setMapper(new WordCountMapperV3());

        mapDriver.addInput(new LongWritable(0), TEST_INPUT_1);
        mapDriver.addInput(new LongWritable(7), TEST_INPUT_2);

        mapDriver.addOutput(new Text("in"),     new IntWritable(2));
        mapDriver.addOutput(new Text("a"),      new IntWritable(2));
        mapDriver.addOutput(new Text("hole"),   new IntWritable(1));
        mapDriver.addOutput(new Text("the"),    new IntWritable(1));
        mapDriver.addOutput(new Text("ground"), new IntWritable(1));
        mapDriver.addOutput(new Text("there"),  new IntWritable(1));
        mapDriver.addOutput(new Text("lived"),  new IntWritable(1));
        mapDriver.addOutput(new Text("hobbit"), new IntWritable(1));

        mapDriver.addOutput(new Text("the"),    new IntWritable(1));
        mapDriver.addOutput(new Text("world"),  new IntWritable(1));
        mapDriver.addOutput(new Text("is"),     new IntWritable(1));
        mapDriver.addOutput(new Text("indeed"), new IntWritable(1));
        mapDriver.addOutput(new Text("full"),   new IntWritable(1));
        mapDriver.addOutput(new Text("of"),     new IntWritable(1));
        mapDriver.addOutput(new Text("peril"),  new IntWritable(1));
        mapDriver.addOutput(new Text("there"),  new IntWritable(1));
        mapDriver.addOutput(new Text("and"),    new IntWritable(1));
        mapDriver.addOutput(new Text("in"),     new IntWritable(1));
        mapDriver.addOutput(new Text("it"),     new IntWritable(1));
        mapDriver.addOutput(new Text("are"),    new IntWritable(1));
        mapDriver.addOutput(new Text("many"),   new IntWritable(1));
        mapDriver.addOutput(new Text("dark"),   new IntWritable(1));
        mapDriver.addOutput(new Text("places"), new IntWritable(1));

        mapDriver.runTest(false); // <= ignore order of emitted keys
    }

    @Test
    public void testReducer() throws Exception {
        final ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver =
                new ReduceDriver<>();

        reduceDriver.setReducer(new WordCountReducerV2());

        final Text key1 = new Text("in");

        final List<IntWritable> values1 = new ArrayList<IntWritable>() {{
            add(new IntWritable(1));
            add(new IntWritable(3));
            add(new IntWritable(7));
            // total of 11 occurrence
        }};

        final Text key2 = new Text("hobbit");

        final List<IntWritable> values2 = new ArrayList<IntWritable>() {{
            add(new IntWritable(1));
            add(new IntWritable(2));
            // total of 3 occurrence
        }};

        reduceDriver.addInput(key1, values1);
        reduceDriver.addInput(key2, values2);

        reduceDriver.addOutput(key1, new IntWritable(11));
        reduceDriver.addOutput(key2, new IntWritable(3));

        reduceDriver.runTest(false);
    }

    @Test
    public void testMapReduce() throws Exception {
        final MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver =
                new MapReduceDriver<>();

        mapReduceDriver.setMapper(new WordCountMapperV3());
        mapReduceDriver.setReducer(new WordCountReducerV2());
        mapReduceDriver.setCombiner(new WordCountReducerV2());

        mapReduceDriver.addInput(new LongWritable(0), TEST_INPUT_1);
        mapReduceDriver.addInput(new LongWritable(7), TEST_INPUT_2);

        mapReduceDriver.addOutput(new Text("in"),     new IntWritable(3));
        mapReduceDriver.addOutput(new Text("a"),      new IntWritable(2));
        mapReduceDriver.addOutput(new Text("the"),    new IntWritable(2));
        mapReduceDriver.addOutput(new Text("there"),  new IntWritable(2));
        mapReduceDriver.addOutput(new Text("hole"),   new IntWritable(1));
        mapReduceDriver.addOutput(new Text("ground"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("lived"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("hobbit"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("world"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("is"),     new IntWritable(1));
        mapReduceDriver.addOutput(new Text("indeed"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("full"),   new IntWritable(1));
        mapReduceDriver.addOutput(new Text("of"),     new IntWritable(1));
        mapReduceDriver.addOutput(new Text("peril"),  new IntWritable(1));
        mapReduceDriver.addOutput(new Text("and"),    new IntWritable(1));
        mapReduceDriver.addOutput(new Text("it"),     new IntWritable(1));
        mapReduceDriver.addOutput(new Text("are"),    new IntWritable(1));
        mapReduceDriver.addOutput(new Text("many"),   new IntWritable(1));
        mapReduceDriver.addOutput(new Text("dark"),   new IntWritable(1));
        mapReduceDriver.addOutput(new Text("places"), new IntWritable(1));

        mapReduceDriver.runTest(false); // <= ignore order of emitted keys
    }
}