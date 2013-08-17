package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public interface DistinctTimePeriod<E> extends TimePeriod<E> {

    long getDistinctCount();

}
