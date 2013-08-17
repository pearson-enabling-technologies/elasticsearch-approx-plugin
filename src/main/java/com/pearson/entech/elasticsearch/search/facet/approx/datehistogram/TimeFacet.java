package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.List;

import org.elasticsearch.search.facet.InternalFacet;

public abstract class TimeFacet<P> extends InternalFacet {

    public TimeFacet(final String name) {
        super(name);
    }

    public abstract long getTotal();

    public abstract List<P> getTimePeriods();

}
