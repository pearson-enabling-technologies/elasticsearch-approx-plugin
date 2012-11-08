package com.pearson.entech.elasticsearch.plugin.approx.termlist;

import java.util.List;

import org.elasticsearch.search.facet.Facet;

public interface TermListFacet extends Facet, Iterable<Object> {

    /**
     * The type of the facet.
     */
    public static final String TYPE = "term_list";

    /**
     * An ordered list of term list facet entries.
     */
    List<? extends Object> entries();

    /**
     * An ordered list of term list facet entries.
     */
    List<? extends Object> getEntries();

}
