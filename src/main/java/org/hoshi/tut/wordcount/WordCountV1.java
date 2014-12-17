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
package org.hoshi.tut.wordcount;

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
 * Classic Hadoop MapReduce 'Word count' example.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountV1 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCountV1.class);

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration configuration = this.getConf();

        final Job job = new Job(configuration, "word-count-v1");

        job.setJarByClass(WordCountV1.class);

        job.setMapperClass(WordCountMapperV1.class);
        job.setReducerClass(WordCountReducerV1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        final int res = ToolRunner.run(
                new Configuration(),
                new WordCountV1(),
                args);

        System.exit(res);
    }
}