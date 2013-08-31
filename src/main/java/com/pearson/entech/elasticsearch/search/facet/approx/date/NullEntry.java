package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public enum NullEntry implements ToXContent {

    INSTANCE;

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return builder;
    }

}
