package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class Slice<L> implements ToXContent {

    private final L _label;
    private final long _totalCount;

    public Slice(final L label, final long totalCount) {
        _label = label;
        _totalCount = totalCount;
    }

    public L getLabel() {
        return _label;
    }

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

    protected void injectSliceXContent(final XContentBuilder builder) throws IOException {
        // override to add extra content in a slice object
    }

}
