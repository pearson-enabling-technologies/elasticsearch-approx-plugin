package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import org.elasticsearch.search.facet.FacetExecutor.Collector;
import org.elasticsearch.search.facet.InternalFacet;

/**
 * A Collector that also provides a build() method.
 */
public abstract class BuildableCollector extends Collector {

    /**
     * Build a facet with the given name from the data collected.
     * 
     * @param facetName the facet name
     * @return the internal facet
     */
    public abstract InternalFacet build(String facetName);

}
