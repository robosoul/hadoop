/**
 * The MIT License (MIT)
 *
 * Copyright (C) 2015 Luka Obradovic.
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
package org.hoshi.tut.hadoop.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class InvertedIndex extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(InvertedIndex.class);

    @Override
    public int run(final String[] args) throws Exception {
        /*
		 * Validate that two arguments were passed from the command line.
		 */
        if (args.length != 2) {
            System.out.printf("Usage: InvertedIndex <input dir> <output dir>\n");
            System.exit(-1);
        }

		/*
		 * Instantiate a Job object for your job's configuration.
		 */
        Job job = new Job(getConf());

		/*
		 * Specify the jar file that contains driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in cluster running
		 * mapper and reducer tasks.
		 */
        job.setJarByClass(InvertedIndex.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
        job.setJobName("Inverted Index");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
        FileInputFormat.setInputPaths(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/*
		 * Specify the mapper and reducer classes.
		 */
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

		/*
		 * The input file and output files are text files, so there is no need
		 * to call the setInputFormatClass and setOutputFormatClass methods.
		 */

		/*
		 * The mapper's output keys and values have the same data types as
		 * reducer's output keys and values. Therefore, no need to call
		 * setMapOutputKeyClass and setMapOutputValueClass methods.
		 */
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);

		/*
		 * Specify the job's output key and value classes.
		 */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    public static void main(final String[] args) throws Exception {
        final int status =
                ToolRunner.run(new Configuration(), new InvertedIndex(), args);

        System.exit(status);
    }
}