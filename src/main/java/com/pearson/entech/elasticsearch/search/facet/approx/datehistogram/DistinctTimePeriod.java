package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class DistinctTimePeriod<E> extends TimePeriod<E> {

    private final long _distinctCount;

    public DistinctTimePeriod(final long time, final E entry, final long distinctCount) {
        super(time, entry);
        _distinctCount = distinctCount;
    }

    public long getDistinctCount() {
        return _distinctCount;
    }

}
