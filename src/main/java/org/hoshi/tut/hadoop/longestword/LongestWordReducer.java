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
 */package org.hoshi.tut.hadoop.longestword;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class LongestWordReducer extends Reducer<Text, Text, Text, Text> {
    public static final Logger log = LoggerFactory.getLogger(LongestWordReducer.class);

    private final Text longestWord;

    public LongestWordReducer() {
        longestWord = new Text();
    }

    @Override
    public void reduce(
            final Text key,
            final Iterable<Text> words,
            final Context context)
    throws IOException, InterruptedException {
        String lw = null;

        /**
         * Loop through all the words for input key (firs letter) and find the
         * longest
         */
        for (Text word : words) {
            String w = word.toString();

            if (lw == null || lw.length() < w.length()) {
                lw = w;
            }
        }

        if (lw != null) {
            longestWord.set(lw);
            context.write(key, longestWord);
        }
    }
}
