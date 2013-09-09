package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;


/**
 * Extension of the DateFacet class for distinct counting.
 * 
 * @param <P> the type of the period objects this facet contains
 */
public abstract class DistinctDateFacet<P extends ToXContent> extends DateFacet<P> implements HasDistinct {

    /**
     * Create a new distinct date facet.
     * 
     * @param name the facet name
     */
    public DistinctDateFacet(final String name) {
        super(name);
    }

    @Override
    protected void injectHeaderXContent(final XContentBuilder builder) throws IOException {
        builder.field(Constants.DISTINCT_COUNT, getDistinctCount());
    }

}
