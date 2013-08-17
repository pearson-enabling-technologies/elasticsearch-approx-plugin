package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.List;

import org.elasticsearch.search.facet.InternalFacet;

public abstract class TimeFacet<P> extends InternalFacet {

    private long _totalCount;

    public TimeFacet(final String name) {
        super(name);
    }

    public long getTotalCount() {
        return _totalCount;
    }

    protected void setTotalCount(final long count) {
        _totalCount = count;
    }

    public abstract List<P> getTimePeriods();

}
