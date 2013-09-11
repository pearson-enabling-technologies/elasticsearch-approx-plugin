package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A placeholder for use in facets which don't have any individual entries.
 * Follows the enum singleton pattern. Just implements a no-op toXContent() call.
 */
public enum NullEntry implements ToXContent {

    /**
     * The only instance of NullEntry you'll ever need.
     */
    INSTANCE;

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return builder;
    }

}
