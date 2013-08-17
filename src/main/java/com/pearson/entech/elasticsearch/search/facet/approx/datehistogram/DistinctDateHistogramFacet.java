package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.List;

import org.elasticsearch.search.facet.Facet;

public interface DistinctDateHistogramFacet extends Facet, Iterable<DistinctDateHistogramFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "distinct_date_histogram";

    /**
     * An ordered list of histogram facet entries.
     */
    List<? extends Entry> getEntries();

    public long getDistinctCount();

    public long getTotalCount();

    public interface Entry {

        /**
         * The time bucket start (in milliseconds).
         */
        long getTime();

        /**
         * The number of distinct values that fall within that key "range" or "interval".
         */
        long getDistinctCount();

        /**
         * The total number of hits that fall within that key "range" or "interval".
         */
        long getTotalCount();

    }

}