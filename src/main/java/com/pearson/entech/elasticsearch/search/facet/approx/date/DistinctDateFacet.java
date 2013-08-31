package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public abstract class DistinctDateFacet<P extends ToXContent> extends DateFacet<P> implements HasDistinct {

    public DistinctDateFacet(final String name) {
        super(name);
    }

    @Override
    protected void injectHeaderXContent(final XContentBuilder builder) throws IOException {
        builder.field(Constants.DISTINCT_COUNT, getDistinctCount());
    }

}
