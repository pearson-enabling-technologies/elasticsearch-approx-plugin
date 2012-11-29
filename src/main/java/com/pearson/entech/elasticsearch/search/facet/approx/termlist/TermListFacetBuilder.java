package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.AbstractFacetBuilder;

public class TermListFacetBuilder extends AbstractFacetBuilder {

    protected TermListFacetBuilder(final String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    // TODO implement field, keyField, maxExactPerShard
    // TODO copy scope, facetFilter and nested from DistinctDateHistogramFacetBuilder

}
