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
package org.hoshi.tut.hadoop.longestword;

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
public class FirstLetterMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static final Logger log = LoggerFactory.getLogger(FirstLetterMapper.class);

    public static final Pattern WORD_PATTERN = Pattern.compile("\\W+");

    private final Text firstLetter;
    private final Text word;

    public FirstLetterMapper() {
        this.firstLetter = new Text();
        this.word = new Text();
    }

    @Override
    public void map(final LongWritable key, final Text value, final Context context)
    throws IOException, InterruptedException {
        final String line = value.toString().toLowerCase();

        for (String w : WORD_PATTERN.split(line)) {
            if (!w.isEmpty()) {
                firstLetter.set(w.substring(0, 1));
                word.set(w);

                context.write(firstLetter, word);
            }
        }
    }
}