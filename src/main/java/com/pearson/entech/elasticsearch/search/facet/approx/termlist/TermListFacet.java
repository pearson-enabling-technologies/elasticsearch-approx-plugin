package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.util.List;

import org.elasticsearch.search.facet.Facet;

/**
 * Defines the content and abilities of a facet class.
 */
public interface TermListFacet extends Facet, Iterable<Object> {

    /**
     * The type of the facet.
     */
    public static final String TYPE = "term_list_facet";

    /**
     * An ordered list of term list facet entries.
     */
    List<? extends Object> entries();

    /**
     * An ordered list of term list facet entries.
     */
    List<? extends Object> getEntries();

}
