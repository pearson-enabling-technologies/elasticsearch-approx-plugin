package com.pearson.entech.elasticsearch.plugin.approx.termlist;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

public class TermListFacetProcessor extends AbstractComponent implements FacetProcessor {

    public TermListFacetProcessor(final Settings settings) {
        super(settings);
        // TODO Auto-generated constructor stub
    }

    @Override
    public FacetCollector parse(final String arg0, final XContentParser arg1, final SearchContext arg2) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Facet reduce(final String arg0, final List<Facet> arg1) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String[] types() {
        // TODO Auto-generated method stub
        return null;
    }

}
