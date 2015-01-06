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
package org.hoshi.tut.hadoop.anagram;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Since AnagramReducer can't be used as Combiner, we here write a custom one.
 * This Combiner has 2 purposes:
 *  1) save network bandwidth
 *  2) lower number of values reducer gets (since it needs to aggregate data)
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class AnagramCombiner extends Reducer<Text, Text, Text, Text> {
    public static final Logger log =
            LoggerFactory.getLogger(AnagramCombiner.class);

    final Text previous;

    public AnagramCombiner() {
        previous = new Text();
    }

    @Override
    protected void reduce(
            final Text key,
            final Iterable<Text> values,
            final Context context)
    throws IOException, InterruptedException {
        /*
         * Logic relies on the fact that values coming to combiner will be
         * sorted.
         */
        String prev = null;

        for (Text value : values) {
            String current = value.toString();

            if (prev != null && !prev.equals(current)) {
                previous.set(prev);
                context.write(key, previous);
            }

            prev = current;
        }

        if (prev != null) {
            previous.set(prev);
            context.write(key, previous);
        }
    }
}