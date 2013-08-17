package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class Slice<L> {

    private final L _label;
    private final long _totalCount;

    public Slice(final L label, final long totalCount) {
        _label = label;
        _totalCount = totalCount;
    }

    public L getLabel() {
        return _label;
    }

    // TODO rename to getTotalCount for consistency everywhere
    public long getCount() {
        return _totalCount;
    }

}
