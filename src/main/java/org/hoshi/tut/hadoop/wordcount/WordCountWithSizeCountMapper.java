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
package org.hoshi.tut.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A WordCountMapper with counters demo. Counters will count how many times a
 * word size of x has occurred.
 *
 * @author Luka Obradovic (obradovic.luka.83@gmail.com)
 */
public class WordCountWithSizeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static final Logger log =
            LoggerFactory.getLogger(WordCountWithSizeCountMapper.class);

    public static final String WORD_SIZE_COUNTER_GROUP = "Word Size Counters";

    /*
     * Use this constant as value of 1.
     */
    private static final IntWritable ONE = new IntWritable(1);

    /*
     * Reuse variable 'word', do not create new one every time.
     * After context.write(), 'word' is serialized, so it's safe to reuse it.
     */
    private final Text word;

    public WordCountWithSizeCountMapper() {
        this.word = new Text();
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
                word.set(w.toLowerCase());
                context.write(word, ONE);

                context.getCounter(WORD_SIZE_COUNTER_GROUP, "length of " + w.length()).increment(1);
            }
        }
    }
}
