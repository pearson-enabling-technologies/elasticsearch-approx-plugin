package com.pearson.entech.elasticsearch.plugin.approx;

import java.util.Collection;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacetParser;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalCountingFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalSlicedDistinctFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.InternalSlicedFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.termlist.InternalTermListFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.termlist.TermListFacetParser;

public class FacetPlugin extends AbstractPlugin {

    public FacetPlugin(final Settings settings) {}

    @Override
    public String name() {
        return "approx-plugin";
    }

    @Override
    public String description() {
        return "Plugin to use approximate methods for enabling and/or speeding up certain queries";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(TimeFacetsModule.class);
        return modules;
    }

    @Override
    public void processModule(final Module module) {
        if(module instanceof FacetModule) {
            ((FacetModule) module).addFacetProcessor(DistinctDateHistogramFacetParser.class);
            InternalCountingFacet.registerStreams();
            InternalDistinctFacet.registerStreams();
            InternalSlicedFacet.registerStreams();
            InternalSlicedDistinctFacet.registerStreams();

            ((FacetModule) module).addFacetProcessor(TermListFacetParser.class);
            InternalTermListFacet.registerStreams();
        }
    }

}
