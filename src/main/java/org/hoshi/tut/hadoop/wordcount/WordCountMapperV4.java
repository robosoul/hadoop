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
 * Similar to WordCountMapperV3 example but we'll use CountSet to count each
 * term in *ALL* inputs for this mapper.
 *
 * Although this solutions looks OK, it's bad practice to buffer data on mappers
 * or reducers. Mappers should only be aware of current input Key Value pair.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountMapperV4 extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final Logger log =
            LoggerFactory.getLogger(WordCountMapperV4.class);

    private CountSet<String> wordsCountSet;

    private final IntWritable count;
    private final Text word;

    public WordCountMapperV4() {
        count = new IntWritable();
        word  = new Text();
    }

    @Override
    protected void setup(final Context context)
    throws IOException, InterruptedException {
        super.setup(context);

        /*
         * Reset at the start.
         */
        wordsCountSet = new CountSet<>();
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

        for (String w : words) {
            if (!w.isEmpty()) {
                wordsCountSet.add(w);
            }
        }
    }

    @Override
    protected void cleanup(final Context context)
    throws IOException, InterruptedException {
        super.cleanup(context);

        for (String w : wordsCountSet.data()) {
            word.set(w);
            count.set(wordsCountSet.count(w));

            context.write(word, count);
        }
    }
}