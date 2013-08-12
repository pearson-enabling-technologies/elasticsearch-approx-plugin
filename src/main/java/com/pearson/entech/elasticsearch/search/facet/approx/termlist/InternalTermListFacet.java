package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import org.elasticsearch.search.facet.InternalFacet;

/**
 * The Class InternalTermListFacet.
 */
public abstract class InternalTermListFacet extends InternalFacet implements TermListFacet {

  public static void registerStreams() {
        InternalStringTermListFacet.registerStream();
    }

    protected InternalTermListFacet() {
    }

    protected InternalTermListFacet(final String facetName) {
        super(facetName);
    }

    @Override
    public final String getType() {
        return TYPE;
    }

}
