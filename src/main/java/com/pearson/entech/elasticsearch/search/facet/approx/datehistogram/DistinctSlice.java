package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public interface DistinctSlice<L> extends Slice<L> {

    long getDistinctCount();

}
