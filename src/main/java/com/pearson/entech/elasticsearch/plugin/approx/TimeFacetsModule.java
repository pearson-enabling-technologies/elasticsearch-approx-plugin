package com.pearson.entech.elasticsearch.plugin.approx;

import org.elasticsearch.common.inject.AbstractModule;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalDistinctDateHistogramFacet;


public class TimeFacetsModule extends AbstractModule {

    @Override
    protected void configure() {
        InternalDistinctDateHistogramFacet.registerStreams();
    }
}
