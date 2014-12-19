/**
 * Copyright Vast 2014. All Rights Reserved.
 *
 * http://www.vast.com
 */
package org.hoshi.tut.hadoop.longestword;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Luka Obradovic (luka@vast.com)
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
