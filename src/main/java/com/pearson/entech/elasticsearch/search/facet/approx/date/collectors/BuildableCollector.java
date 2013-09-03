package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import org.elasticsearch.index.fielddata.LongValues.Iter;
import org.elasticsearch.search.facet.FacetExecutor.Collector;
import org.elasticsearch.search.facet.InternalFacet;

public abstract class BuildableCollector extends Collector {

    protected static final Iter EMPTY = new Iter.Empty();

    public abstract InternalFacet build(String facetName);

}
