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
package org.hoshi.tut.hadoop.times;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.regex.Pattern;

/**
 * "Proper" use of Combiner. (non-working implementation)
 *
 * Combiners must have the same input and output key-value type, which also
 * must be the same as the 'mapper output type' and the 'reducer input type'.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class SessionMeanTimeV2 extends Configured implements Tool {
    public static final Logger log = LoggerFactory.getLogger(SessionMeanTimeV2.class);

    private static class SessionMeanTimeV2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
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

    private static class SessionMeanTimeV2Combiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(
                final Text key,
                final Iterable<DoubleWritable> values,
                final Context context)
        throws IOException, InterruptedException {

        }
    }

    private static class SessionMeanTimeV2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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

        final Job job = new Job(configuration, "session-mean-time-v2");

        job.setJarByClass(SessionMeanTimeV2.class);

        job.setMapperClass(SessionMeanTimeV2Mapper.class);
        job.setCombinerClass(SessionMeanTimeV2Reducer.class);
        job.setReducerClass(SessionMeanTimeV2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true)) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status =
                ToolRunner.run(
                        new Configuration(),
                        new SessionMeanTimeV2(),
                        args);

        System.exit(status);
    }
}