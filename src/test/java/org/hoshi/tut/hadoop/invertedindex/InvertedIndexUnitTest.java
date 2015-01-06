package org.hoshi.tut.hadoop.invertedindex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.Test;

public class InvertedIndexUnitTest {
    public static final Text INPUT_FILE_NAME = new Text("hobbit.txt");
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

    // todo: add reducer and mr tests
}