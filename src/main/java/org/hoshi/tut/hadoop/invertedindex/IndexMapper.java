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
package org.hoshi.tut.hadoop.invertedindex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.hoshi.tut.hadoop.wordcount.WordCountMapperV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * A mapper class that will map word to it's location (file name, url, etc...)
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static final Logger log = LoggerFactory.getLogger(IndexMapper.class);

    public static final Pattern WORDS_SPLITTER = Pattern.compile("\\W+");

    private final Text word;
    private final Text location;

    public IndexMapper() {
        word   = new Text();
        location = new Text();
    }

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context)
    throws IOException, InterruptedException {
        /*
         * Get the FileSplit for the input file, which provides access to the
         * file's path. We need the file's path because it contains the name of
         * the file.
         */
        final FileSplit split = (FileSplit) context.getInputSplit();
        final Path inputPath = split.getPath();

        location.set(inputPath.getName());

        final String[] words =
                WordCountMapperV1.WORDS_SPLITTER.split(value.toString().toLowerCase());

        for (String w : words) {
            if (!w.isEmpty()) {
                word.set(w.toLowerCase());
                context.write(word, location);
            }
        }
    }
}