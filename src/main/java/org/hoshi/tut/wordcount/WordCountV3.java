/**
 * Copyright Vast 2014. All Rights Reserved.
 *
 * http://www.vast.com
 */
package org.hoshi.tut.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.StringTokenizer;

/**
 * Same as WordCountV2 example, except we'll use a combiner (local aggregation optimization)
 *
 * @author Luka Obradovic (luka@vast.com)
 */
public class WordCountV3 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(WordCountV3.class);

    public static class WordCountV3Mapper extends Mapper<Object, Text, Text, IntWritable> {
        // use this constant as value of 1
        private static final IntWritable ONE = new IntWritable(1);

        // reuse variable 'word', do not create new one every time
        private Text word = new Text();

        @Override
        public void map(
                final Object key,
                final Text value,
                final Context context)
        throws IOException, InterruptedException {
            final StringTokenizer words = new StringTokenizer(value.toString());

            while (words.hasMoreTokens()) {
                word.set(words.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static class WordCountV3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

        final Job job = new Job(conf, "word-count-v3");

        job.setJarByClass(WordCountV3.class);

        job.setMapperClass(WordCountV3Mapper.class);
        job.setReducerClass(WordCountV3Reducer.class);

        // we'll use combiner to locally aggregate data before sending
        // it to reducers
        job.setCombinerClass(WordCountV3Reducer.class);

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
