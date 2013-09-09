package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.util.List;

import org.elasticsearch.search.facet.Facet;

/**
 * Terms facet allows to return facets of the most popular terms within the search query.
 */
public interface TermListFacet extends Facet, Iterable<String> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "term_list";

    /**
     * The terms and counts.
     */
    List<? extends String> getEntries();

}