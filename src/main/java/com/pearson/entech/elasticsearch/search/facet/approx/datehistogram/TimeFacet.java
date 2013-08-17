package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.List;

public interface TimeFacet<P> {

    long getTotal();

    List<P> getTimePeriods();

}
