/**
 * Copyright Vast 2014. All Rights Reserved.
 *
 * http://www.vast.com
 */
package org.hoshi.tut.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Same as WordCountV2 example, except we'll use a combiner (local aggregation
 * optimization)
 *
 * @author Luka Obradovic (luka@vast.com)
 */
public class WordCountV3 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCountV3.class);

    @Override
    public int run(final String[] args) throws Exception {
        /*
		 * Validate that two arguments were passed from the command line.
		 */
        if (args.length != 2) {
            System.out.println("Usage: WordCountV3 <input dir> <output dir>\n");
            System.exit(-1);
        }

        final Job job = new Job(getConf(), "word-count-v3");

        /*
         * Specify the jar file that contains your driver, mapper, and reducer.
         * Hadoop will transfer this jar file to nodes in cluster running mapper
         * and reducer tasks.
         */
        job.setJarByClass(WordCountV3.class);

        /*
         * Specify the paths to the input and output data based on the
         * command-line arguments.
         */
        FileInputFormat.setInputPaths(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * Specify the mapper, reducer (and combiner) classes.
         */
        job.setMapperClass(WordCountMapperV2.class);
        job.setReducerClass(WordCountReducerV2.class);

        /*
         * Use combiner to locally aggregate data before sending it to reducers.
         */
        job.setCombinerClass(WordCountReducerV2.class);

        /*
         * For the word count application, the mapper's output keys and
         * values have the same data types as the reducer's output keys
         * and values: Text and IntWritable.
         *
         * When they are not the same data types, setMapOutputKeyClass and
         * setMapOutputValueClass methods must be called.
         */

        /*
         * Specify the job's output key and value classes.
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(
                new Configuration(),
                new WordCountV3(),
                args);

        System.exit(res);
    }
}
