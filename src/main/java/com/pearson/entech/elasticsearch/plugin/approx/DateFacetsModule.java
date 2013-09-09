package com.pearson.entech.elasticsearch.plugin.approx;

import org.elasticsearch.common.inject.AbstractModule;

import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalCountingFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalSlicedDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalSlicedFacet;

/**
 * DI module for registering the facet types with ElasticSearch's serialization mechanisms.
 * 
 */
public class DateFacetsModule extends AbstractModule {

    @Override
    protected void configure() {
        InternalCountingFacet.registerStreams();
        InternalDistinctFacet.registerStreams();
        InternalSlicedFacet.registerStreams();
        InternalSlicedDistinctFacet.registerStreams();
    }

}
