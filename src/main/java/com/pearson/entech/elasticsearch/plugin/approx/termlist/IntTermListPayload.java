package com.pearson.entech.elasticsearch.plugin.approx.termlist;

import org.elasticsearch.common.trove.list.TIntList;
import org.elasticsearch.common.trove.list.array.TIntArrayList;

public class IntTermListPayload implements TermListPayload<int[]> {

    // TODO add capacity planning later?
    TIntList _terms = new TIntArrayList();

    @Override
    public void add(final int[] term) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBulk(final int[] terms) {
        _terms.addAll(terms);
    }

}
