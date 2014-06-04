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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hoshi.tut.util.collect.CountSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Similar to WordCountV4 example but we'll use CountSet to count each term in
 * *ALL* inputs for this Mapper.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountV5 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCountV5.class);

    public static class WordCountV5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private CountSet<String> wordsCountSet;

        private final IntWritable count = new IntWritable();
        private final Text word = new Text();

        @Override
        protected void setup(final Context context)
        throws IOException, InterruptedException {
            super.setup(context);

            wordsCountSet = new CountSet<String>();
        }

        @Override
        protected void map(
                final LongWritable key,
                final Text value,
                final Context context)
        throws IOException, InterruptedException {
            final StringTokenizer tokenizer =
                    new StringTokenizer(value.toString());

            final CountSet<String> wordsCountSet = new CountSet<String>();

            while (tokenizer.hasMoreTokens()) {
                wordsCountSet.add(tokenizer.nextToken());
            }
        }

        @Override
        protected void cleanup(final Context context)
        throws IOException, InterruptedException {
            super.cleanup(context);

            for (String s : wordsCountSet.data()) {
                word.set(s);
                count.set(wordsCountSet.count(s));

                context.write(word, count);
            }
        }
    }
    public static class WordCountV5Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reuse variable 'word', do not create new one every time
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

        final Job job = new Job(conf, "word-count-v5");

        job.setJarByClass(WordCountV5.class);

        job.setMapperClass(WordCountV5Mapper.class);
        job.setReducerClass(WordCountV5Reducer.class);

        //job.setCombinerClass(WordCountV4Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(
                new Configuration(),
                new WordCountV2(),
                args);

        System.exit(res);
    }
}