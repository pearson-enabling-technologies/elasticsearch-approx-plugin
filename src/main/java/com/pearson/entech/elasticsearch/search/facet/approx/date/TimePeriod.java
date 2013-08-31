package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class TimePeriod<E extends ToXContent> implements ToXContent {

    private final long _time;
    private final long _totalCount;
    private final E _entry;

    public TimePeriod(final long time, final long totalCount, final E entry) {
        _time = time;
        _totalCount = totalCount;
        _entry = entry;
    }

    public long getTime() {
        return _time;
    }

    public long getTotalCount() {
        return _totalCount;
    }

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

    protected void injectEntryHeaderXContent(final XContentBuilder builder) throws IOException {
        // override to add extra content immediately before the entry for this period
    }

    protected void injectEntryFooterXContent(final XContentBuilder builder) throws IOException {
        // override to add extra content immediately after the entry for this period
    }

}
