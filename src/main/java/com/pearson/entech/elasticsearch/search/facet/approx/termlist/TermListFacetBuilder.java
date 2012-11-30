package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
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

    /**
     * Marks the facet to run in a specific scope.
     */
    @Override
    public TermListFacetBuilder scope(final String scope) {
        super.scope(scope);
        return this;
    }

    /**
     * An additional filter used to further filter down the set of documents the facet will run on.
     */
    @Override
    public TermListFacetBuilder facetFilter(final FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    @Override
    public TermListFacetBuilder nested(final String nested) {
        this.nested = nested;
        return this;
    }

}
