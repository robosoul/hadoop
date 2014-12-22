/**
 * Copyright (C) 2014 Luka Obradovic.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
 * Same as WordCountV3 example, except we'll use a CountSet (local aggregation
 * optimization in Mapper) to count each term in input, and emmit only unique
 * terms and their counts.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountV4 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCountV4.class);

    @Override
    public int run(final String[] args) throws Exception {
        /*
		 * Validate that two arguments were passed from the command line.
		 */
        if (args.length != 2) {
            System.out.println("Usage: WordCountV4 <input dir> <output dir>\n");
            System.exit(-1);
        }

        /*
         * Instantiate a Job object with configuration and a name. This job name
         * will appear in reports and logs.
         */
        final Job job = new Job(getConf(), "word-count-v4");

        /*
         * Specify the jar file that contains your driver, mapper, and reducer.
         * Hadoop will transfer this jar file to nodes in cluster running mapper
         * and reducer tasks.
         */
        job.setJarByClass(WordCountV4.class);

        /*
         * Specify the paths to the input and output data based on the
         * command-line arguments.
         */
        FileInputFormat.setInputPaths(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * Specify the mapper, combiner and reducer classes.
         */
        job.setMapperClass(WordCountMapperV3.class);
        job.setCombinerClass(WordCountReducerV2.class);
        job.setReducerClass(WordCountReducerV2.class);

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
                new WordCountV4(),
                args);

        System.exit(res);
    }
}