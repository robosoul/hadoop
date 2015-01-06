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
package org.hoshi.tut.hadoop.wordsize;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordSizeMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    public static final Logger log =
            LoggerFactory.getLogger(WordSizeMapper.class);

    public static final Pattern WORDS_SPLITTER = Pattern.compile("\\W+");

    private static final IntWritable ONE = new IntWritable(1);

    private final IntWritable length;

    public WordSizeMapper() {
        length = new IntWritable(0);
    }

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context)
    throws IOException, InterruptedException {
        final String[] words =
                WORDS_SPLITTER.split(value.toString().toLowerCase());

        for (String word : words) {
            if (!word.isEmpty()) {
                length.set(word.length());
                context.write(length, ONE);
            }
        }
    }
}