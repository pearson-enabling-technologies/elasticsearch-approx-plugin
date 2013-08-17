package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public interface DistinctTimeFacet<P> extends TimeFacet<P> {

    long getDistinctCount();

}
