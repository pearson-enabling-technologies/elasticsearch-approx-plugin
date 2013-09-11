package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A slice which reports distinct values.
 * 
 * @param <L> the data type of the slice label
 */
public class DistinctSlice<L> extends Slice<L> implements HasDistinct {

    private final long _distinctCount;

    /**
     * Create a new DistinctSlice.
     * 
     * @param label the slice label to use
     * @param count the count of values in this slice
     * @param distinctCount the count of distinct values in this slice
     */
    public DistinctSlice(final L label, final long count, final long distinctCount) {
        super(label, count);
        _distinctCount = distinctCount;
    }

    @Override
    public long getDistinctCount() {
        return _distinctCount;
    }

    @Override
    protected void injectSliceXContent(final XContentBuilder builder) throws IOException {
        builder.field(Constants.DISTINCT_COUNT, getDistinctCount());
    }

}
