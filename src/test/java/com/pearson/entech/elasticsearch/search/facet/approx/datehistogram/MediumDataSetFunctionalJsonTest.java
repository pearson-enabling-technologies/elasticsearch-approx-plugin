package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import org.junit.Ignore;
import org.junit.Test;

public class MediumDataSetFunctionalJsonTest extends MediumDataSetTest {

    @Test
    public void testMinuteIntervalUnboundedStringExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "minute_interval_unbounded_string");
    }

    // doubles not yet officially supported
    @Ignore
    @Test
    public void testHourIntervalUnboundedDoubleExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "hour_interval_unbounded_double");
    }

    @Ignore
    @Test
    public void testHourIntervalUnboundedDoubleListExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "hour_interval_unbounded_double_list");
    }

    @Test
    public void testMinuteIntervalUnboundedLongExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "minute_interval_unbounded_long");
    }

    @Test
    public void testDayIntervalLondonUnboundedExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "day_interval_london_unbounded_boolean");
    }

    @Test
    public void testDayIntervalKolkataUnboundedExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "day_interval_kolkata_unbounded_boolean");
    }

}
