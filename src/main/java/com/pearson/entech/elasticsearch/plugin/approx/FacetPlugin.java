package com.pearson.entech.elasticsearch.plugin.approx;

import java.util.Collection;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.facet.FacetModule;

import com.pearson.entech.elasticsearch.facet.approx.datehistogram.DistinctDateHistogramFacetParser;


public class FacetPlugin extends AbstractPlugin {

    public FacetPlugin(final Settings settings) {}

    @Override
    public String name() {
        return "time-facets";
    }

    @Override
    public String description() {
        return "Time-Facets Plugins";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        final Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(TimeFacetsModule.class);
        return modules;
    }

    public void onModule(final FacetModule module) {
        module.addFacetProcessor(DistinctDateHistogramFacetParser.class);
    }
}
