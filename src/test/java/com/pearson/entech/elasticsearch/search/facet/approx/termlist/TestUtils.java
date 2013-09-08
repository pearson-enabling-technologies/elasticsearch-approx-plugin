package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.Random;

public class TestUtils {

    public static final Random RANDOM = new Random(0);

    public static List<Integer> generateRandomInts(final int numOfElements) {
        final List<Integer> ret = newArrayList();
        for(int i = 0; i < numOfElements; i++) {
            ret.add(RANDOM.nextInt(1000));
        }
        return ret;
    }

    public static List<Long> generateRandomLongs(final int numOfElements) {
        final List<Long> ret = newArrayList();
        for(int i = 0; i < numOfElements; i++) {
            final long val = RANDOM.nextInt(10000);
            ret.add(val);
        }
        return ret;
    }

    public static List<String> generateRandomWords(final int numberOfWords) {
        final String[] randomStrings = new String[numberOfWords];
        for(int i = 0; i < numberOfWords; i++)
        {
            final char[] word = new char[RANDOM.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for(int j = 0; j < word.length; j++)
            {
                word[j] = (char) ('a' + RANDOM.nextInt(26));
            }
            randomStrings[i] = new String(word);
        }
        return newArrayList(randomStrings);
    }

}
