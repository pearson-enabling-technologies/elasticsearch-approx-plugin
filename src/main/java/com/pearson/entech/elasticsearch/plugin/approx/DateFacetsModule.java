package com.pearson.entech.elasticsearch.plugin.approx;

import org.elasticsearch.common.inject.AbstractModule;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalCountingFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalSlicedDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalSlicedFacet;

public class DateFacetsModule extends AbstractModule {

    @Override
    protected void configure() {
        InternalCountingFacet.registerStreams();
        InternalDistinctFacet.registerStreams();
        InternalSlicedFacet.registerStreams();
        InternalSlicedDistinctFacet.registerStreams();
    }

}
