/*
 * Copyright (C) 2014 Luka Obradovic
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

/**
 * Copyright Vast 2014. All Rights Reserved.
 *
 * http://www.vast.com
 */
package org.hoshi.tut.genome;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Luka Obradovic (luka@vast.com)
 */
public class KmerCount extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(KmerCount.class);

    public static class KmerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(
                final LongWritable key,
                final Text value,
                final Context context)
        throws IOException, InterruptedException {
            if (value.charAt(0) != '>') {
                final String s = value.toString();
                int length = s.length();

                for (int i = 0; i < length - 3; i += 3) {
                    String sub = s.substring(i, i + 3);
                    context.write(new Text(sub), ONE);
                }
            }
        }
    }

    public static class KmerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(
                final Text key,
                final Iterable<IntWritable> values,
                final Context context)
        throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();

        final Job job = new Job(conf, "k-mer count");

        job.setJarByClass(KmerCount.class);

        job.setMapperClass(KmerMapper.class);
        job.setReducerClass(KmerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(
                new Configuration(),
                new KmerCount(),
                args);

        System.exit(res);
    }
}
