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
package org.hoshi.tut.times;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Calculates mean session time (in seconds) for each user. Input format
 * user:time (milan:53).
 *
 * Mean(1,2,3,4,5) != Mean(Mean(1,2),Mean(3,4,5))
 *
 * with above mentioned Reducer class can't be used as Combiner.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class SessionMeanTimeV1 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(SessionMeanTimeV1.class);

    private static class SessionMeanTimeV1Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text name = new Text();
        private final DoubleWritable time = new DoubleWritable();

        public static final Pattern SEPARATOR = Pattern.compile(":");

        @Override
        protected void map(
                final LongWritable key,
                final Text value,
                final Context context)
        throws IOException, InterruptedException {
            final String[] input = SEPARATOR.split(value.toString());

            name.set(input[0]);
            time.set(Double.parseDouble(input[1]));

            context.write(name, time);
        }
    }

    private static class SessionMeanTimeV1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable mean = new DoubleWritable();

        @Override
        protected void reduce(
                final Text name,
                final Iterable<DoubleWritable> times,
                final Context context)
        throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable time : times) {
                sum += time.get();
                ++count;
            }

            mean.set(sum / count);
            context.write(name, mean);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration configuration = this.getConf();

        final Job job = new Job(configuration, "session-mean-time-v1");

        job.setJarByClass(SessionMeanTimeV1.class);

        job.setMapperClass(SessionMeanTimeV1Mapper.class);
        job.setReducerClass(SessionMeanTimeV1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job,   new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status =
                ToolRunner.run(
                    new Configuration(),
                    new SessionMeanTimeV1(),
                    args);

        System.exit(status);
    }
}