package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public interface Slice<L> {

    L getLabel();

    long getCount();

}
