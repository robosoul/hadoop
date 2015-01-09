package org.hoshi.tut.hadoop.invertedindex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class InvertedIndexUnitTest {
    public static final Text INPUT_FILE_NAME = new Text("Hobbit.txt");
    public static final String INPUT_FILE_PATH = "books/jrrt/" + INPUT_FILE_NAME;

    public static final Text TEST_INPUT_1 = new Text("In a hole in the ground there lived a hobbit.");

    @Test
    public void testMapper() throws Exception {
        final MapDriver<LongWritable, Text, Text, Text> mapDriver = new MapDriver<>();

        mapDriver.setMapper(new IndexMapper());

        mapDriver.setMapInputPath(new Path(INPUT_FILE_PATH));
        mapDriver.setInput(new LongWritable(0), TEST_INPUT_1);

        mapDriver.addOutput(new Text("in"),     INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("a"),      INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("hole"),   INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("in"),     INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("the"),    INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("ground"), INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("there"),  INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("lived"),  INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("a"),      INPUT_FILE_NAME);
        mapDriver.addOutput(new Text("hobbit"), INPUT_FILE_NAME);

        mapDriver.runTest(false);
    }

    @Test
    public void testReducer() throws Exception {
        final ReduceDriver<Text, Text, Text, Text> reduceDriver = new ReduceDriver<>();

        reduceDriver.setReducer(new IndexReducer());

        final Text key1 = new Text("Bilbo");
        reduceDriver.addInput(key1, new ArrayList<Text>(4) {{
                                  add(new Text("Hobbit.txt"));
                                  add(new Text("LOTR-FOTR.txt"));
                                  add(new Text("LOTR-ROTK.txt"));
                                  add(new Text("LOTR-TTT.txt"));
                              }});

        final Text key2 = new Text("Aragorn");
        reduceDriver.addInput(key2, new ArrayList<Text>(4) {{
                                  add(new Text("LOTR-FOTR.txt"));
                                  add(new Text("LOTR-ROTK.txt"));
                                  add(new Text("LOTR-TTT.txt"));
                              }});

        reduceDriver.addOutput(key1, new Text("Hobbit.txt,LOTR-FOTR.txt,LOTR-ROTK.txt,LOTR-TTT.txt"));
        reduceDriver.addOutput(key2, new Text("LOTR-FOTR.txt,LOTR-ROTK.txt,LOTR-TTT.txt"));

        reduceDriver.runTest(false);
    }
}