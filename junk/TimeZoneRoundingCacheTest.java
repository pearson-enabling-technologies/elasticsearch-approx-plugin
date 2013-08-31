package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.joda.time.Chronology;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.chrono.ISOChronology;
import org.junit.Test;

public class TimeZoneRoundingCacheTest {

    @Test
    public void testCacheMonotonically() throws Exception {
        final int cacheSize = 1000;
        final int reps = 2000;
        final long[] input = new long[reps];
        final long[] expected = new long[reps];
        final long[] actual = new long[reps];
        final Chronology chronology = ISOChronology.getInstanceUTC();
        final TimeZoneRounding tzRounding = new TimeZoneRounding.Builder(chronology.secondOfMinute())
                .preZone(DateTimeZone.forID("Australia/Sydney"))
                .build();
        final TimeZoneRoundingCache cache = new TimeZoneRoundingCache(tzRounding, cacheSize);

        for(int i = 0; i < reps; i++) {
            input[i] = System.currentTimeMillis() / 1000;
            Thread.sleep(1);
        }

        final long start1 = System.currentTimeMillis();
        for(int i = 0; i < reps; i++) {
            expected[i] = tzRounding.calc(input[i]);
        }
        final long time1 = System.currentTimeMillis() - start1;

        final long start2 = System.currentTimeMillis();
        for(int i = 0; i < reps; i++) {
            actual[i] = cache.round(input[i]);
        }
        final long time2 = System.currentTimeMillis() - start2;

        assertTrue(time2 < time1);
        assertArrayEquals(expected, actual);
    }
}
