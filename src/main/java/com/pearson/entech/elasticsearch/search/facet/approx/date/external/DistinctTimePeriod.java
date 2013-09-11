package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A time period which reports distinct values.
 * 
 * @param <E> the data type of the time period entry
 */
public class DistinctTimePeriod<E extends ToXContent> extends TimePeriod<E> {

    private final long _distinctCount;

    /**
     * Create a new DistinctTimePeriod.
     * 
     * @param time the timestamp of this period
     * @param count the count of values in this period
     * @param distinctCount the count of distinct values in this period
     * @param entry the actual facet entry for this time period
     */
    public DistinctTimePeriod(final long time, final long count,
            final long distinctCount, final E entry) {
        super(time, count, entry);
        _distinctCount = distinctCount;
    }

    /**
     * Get the distinct count for this time period.
     * 
     * @return the distinct count
     */
    public long getDistinctCount() {
        return _distinctCount;
    }

    @Override
    protected void injectEntryHeaderXContent(final XContentBuilder builder) throws IOException {
        builder.field(Constants.DISTINCT_COUNT, getDistinctCount());
    }

    @Override
    protected void injectEntryFooterXContent(final XContentBuilder builder) throws IOException {}

}
