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
 * Configurable WordCount job. Run job with: '-Dcase-sensitive=true'.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountV6 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCountV6.class);

    @Override
    public int run(final String[] args) throws Exception {
        /*
		 * Validate that two arguments were passed from the command line.
		 */
        if (args.length != 2) {
            System.out.println("Usage: WordCountV6 <input dir> <output dir>\n");
            return -1;
        }

        final Job job = new Job(getConf(), "word-count-v6");

        /*
         * Specify the jar file that contains your driver, mapper, and reducer.
         * Hadoop will transfer this jar file to nodes in cluster running mapper
         * and reducer tasks.
         */
        job.setJarByClass(WordCountV6.class);

        /*
         * Specify the paths to the input and output data based on the
         * command-line arguments.
         */
        FileInputFormat.setInputPaths(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * Specify the mapper and reducer classes.
         */
        job.setMapperClass(WordCountCaseSensitiveMapper.class);
        job.setReducerClass(WordCountReducerV2.class);

        /*
         * No need for combiner since data is aggregated on mapper.
         */
        //job.setCombinerClass(WordCountReducerV2.class);

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
        final int status =
                ToolRunner.run(new Configuration(), new WordCountV6(), args);

        System.exit(status);
    }
}