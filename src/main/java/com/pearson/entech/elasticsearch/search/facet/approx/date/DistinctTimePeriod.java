package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class DistinctTimePeriod<E extends ToXContent> extends TimePeriod<E> {

    private final long _distinctCount;

    public DistinctTimePeriod(final long time, final long totalCount,
            final long distinctCount, final E entry) {
        super(time, totalCount, entry);
        _distinctCount = distinctCount;
    }

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
