package com.pearson.entech.elasticsearch.plugin.approx;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacetProcessor;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalDistinctDateHistogramFacet;

public class FacetPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "approx-plugin";
    }

    @Override
    public String description() {
        return "blah blah"; // TODO
    }

    @Override
    public void processModule(final Module module) {
        if(module instanceof FacetModule) {
            ((FacetModule) module).addFacetProcessor(DistinctDateHistogramFacetProcessor.class);
            InternalDistinctDateHistogramFacet.registerStreams();
        }
    }
}
