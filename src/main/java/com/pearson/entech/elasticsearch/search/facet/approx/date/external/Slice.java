package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A slice in a sliced facet.
 * 
 * @param <L> the data type of the slice label
 */
public class Slice<L> implements ToXContent {

    private final L _label;
    private final long _totalCount;

    /**
     * Create a new slice.
     * 
     * @param label the label
     * @param totalCount the count of values in this slice
     */
    public Slice(final L label, final long totalCount) {
        _label = label;
        _totalCount = totalCount;
    }

    /**
     * Get the label for this slice.
     * 
     * @return the label
     */
    public L getLabel() {
        return _label;
    }

    /**
     * Get the value count for this slice.
     * 
     * @return the count
     */
    public long getTotalCount() {
        return _totalCount;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(Constants.TERM, getLabel());
        builder.field(Constants.COUNT, getTotalCount());
        injectSliceXContent(builder);
        builder.endObject();
        return builder;
    }

    /**
     * Override this method to inject additional fields after the label and count,
     * e.g. distinct counts. This method will be called at the appropriate time
     * by Slice's own toXContent() method.
     * 
     * @param builder an XContentBuilder to use
     * @throws IOException
     */
    protected void injectSliceXContent(final XContentBuilder builder) throws IOException {}

}
