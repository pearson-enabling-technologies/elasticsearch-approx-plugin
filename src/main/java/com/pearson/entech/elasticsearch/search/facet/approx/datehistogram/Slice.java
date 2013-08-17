package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class Slice<L> {

    private final L _label;
    private final long _count;

    public Slice(final L label, final long count) {
        _label = label;
        _count = count;
    }

    public L getLabel() {
        return _label;
    }

    long getCount() {
        return _count;
    }

}
