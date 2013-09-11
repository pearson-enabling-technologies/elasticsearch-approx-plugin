package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

/**
 * An interface for objects which report a distinct count.
 */
public interface HasDistinct {

    /**
     * Get the distinct count.
     * 
     * @return the distinct count
     */
    long getDistinctCount();

}
