package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A time period within a date facet. Every time period has an "entry" associated
 * it which is the content to render within this time period. Use NullEntry for
 * facets which don't contain any additional content -- i.e. standard counting
 * facets, where each time period only contains a timestamp and a count.
 * 
 * @param <E> the data type of the time period facet entry
 */
public class TimePeriod<E extends ToXContent> implements ToXContent {

    private final long _time;
    private final long _totalCount;
    private final E _entry;

    /**
     * Create a new TimePeriod.
     * 
     * @param time the timestamp
     * @param totalCount the count of total values in this period
     * @param entry the 
     */
    public TimePeriod(final long time, final long totalCount, final E entry) {
        _time = time;
        _totalCount = totalCount;
        _entry = entry;
    }

    /**
     * Get the timestamp for the start of this period.
     * 
     * @return the timestamp (millis since epoch)
     */
    public long getTime() {
        return _time;
    }

    /**
     * Get the count of total values in this time period.
     * 
     * @return the count
     */
    public long getTotalCount() {
        return _totalCount;
    }

    /**
     * Get the additional facet data to render within this time period.
     * 
     * @return the facet entry
     */
    public E getEntry() {
        return _entry;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(Constants.TIME, getTime());
        builder.field(Constants.COUNT, getTotalCount());
        injectEntryHeaderXContent(builder);
        getEntry().toXContent(builder, params);
        injectEntryFooterXContent(builder);
        builder.endObject();
        return builder;
    }

    /**
     * Override this method to inject additional fields after the timestamp and count,
     * but before the entry data, e.g. distinct counts. This method will be called
     * at the appropriate time by TimePeriod's own toXContent() method.
     * 
     * @param builder an XContentBuilder to use
     * @throws IOException
     */
    protected void injectEntryHeaderXContent(final XContentBuilder builder) throws IOException {}

    /**
     * Override this method to inject additional fields after the entry data.
     * This method will be called at the appropriate time by TimePeriod's own toXContent() method.
     * 
     * @param builder an XContentBuilder to use
     * @throws IOException
     */
    protected void injectEntryFooterXContent(final XContentBuilder builder) throws IOException {}

}
