/**
 * The MIT License (MIT)
 *
 * Copyright (C) 2014 Luka Obradovic.
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
package org.hoshi.tut.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.hoshi.tut.hadoop.util.collect.CountSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Same as WordCountMapperV2 example, except we'll use a count map (local
 * aggregation optimization in mapper) to count each term in input, and emmit
 * only unique terms and their counts.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountMapperV3 extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final Logger log = LoggerFactory.getLogger(WordCountMapperV3.class);

    private final IntWritable count;
    private final Text word;

    public WordCountMapperV3() {
        count = new IntWritable();
        word  = new Text();
    }

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context)
    throws IOException, InterruptedException {

        final String lineLowerCased = value.toString().toLowerCase();

        final String[] words =
                WordCountMapperV1.WORDS_SPLITTER.split(lineLowerCased);

        final CountSet<String> wordsCountSet = new CountSet<>();

        for (String w : words) {
            // DO NOT EMMIT A KEY-VALUE PAIR FOR EACH TERM IN INPUT...
            //context.write(new Text(words.nextToken()), new IntWritable(1));

            if (!w.isEmpty()) {
                wordsCountSet.add(w);
            }
        }

        for (String w : wordsCountSet.data()) {
            word.set(w);
            count.set(wordsCountSet.count(w));

            // ...BUT RATHER EMMIT KEY-VALUE PAIR FOR EACH *UNIQUE* TERM IN INPUT.
            context.write(word, count);
        }
    }
}
