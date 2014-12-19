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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * WordCountMapperV1 with some optimization.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountMapperV2 extends Mapper<IntWritable, Text, Text, IntWritable> {
    public static final Logger log =
            LoggerFactory.getLogger(WordCountMapperV2.class);

    // use this constant as value of 1
    private static final IntWritable ONE = new IntWritable(1);

    // reuse variable 'word', do not create new one every time
    // after context.write(), 'word' is serialized, so it's safe to reuse it
    private final Text word;

    public WordCountMapperV2() {
        this.word = new Text();
    }

    @Override
    protected void map(
            final IntWritable key,
            final Text value,
            final Context context)
    throws IOException, InterruptedException {
        final String lineLowerCased = value.toString().toLowerCase();

        final String[] words =
                WordCountMapperV1.WORDS_SPLITTER.split(lineLowerCased);

        for (String w : words) {
            if (!w.isEmpty()) {
                word.set(w.toLowerCase());
                context.write(word, ONE);
            }
        }
    }
}