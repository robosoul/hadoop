package org.hoshi.tut.hadoop;

import org.apache.commons.lang3.StringUtils;

import java.text.BreakIterator;
import java.util.Locale;

/**
 * Hello world!
 *
 */
public class PlayingAround {
    public static final String TEST_SENTENCE = "In a hole in the ground there lived a hobbit.";

    public static void main( String[] args ) {
        final BreakIterator breakIterator =
                BreakIterator.getWordInstance(Locale.ENGLISH);

        breakIterator.setText(TEST_SENTENCE);

        int start = breakIterator.first();
        int end   = breakIterator.next();

        while (end != BreakIterator.DONE) {
            String word = TEST_SENTENCE.substring(start, end);

            if (StringUtils.isAlpha(word)) {
                System.out.println(word);
            }

            start = end;
            end = breakIterator.next();
        }
    }
}
