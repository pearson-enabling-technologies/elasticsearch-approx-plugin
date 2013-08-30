package com.pearson.entech.elasticsearch.plugin.approx;

import org.elasticsearch.common.inject.AbstractModule;

import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalCountingFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalSlicedDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalSlicedFacet;

public class DateFacetsModule extends AbstractModule {

    @Override
    protected void configure() {
        InternalCountingFacet.registerStreams();
        InternalDistinctFacet.registerStreams();
        InternalSlicedFacet.registerStreams();
        InternalSlicedDistinctFacet.registerStreams();
    }

}
