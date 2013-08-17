package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class DistinctSlice<L> extends Slice<L> implements HasDistinct {

    private final long _distinctCount;

    public DistinctSlice(final L label, final long count, final long distinctCount) {
        super(label, count);
        _distinctCount = distinctCount;
    }

    @Override
    public long getDistinctCount() {
        return _distinctCount;
    }

}
