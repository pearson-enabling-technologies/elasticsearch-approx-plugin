package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class DistinctTimePeriod<E> extends TimePeriod<E> {

    private final long _distinctCount;

    public DistinctTimePeriod(final long time, final long totalCount,
            final long distinctCount, final E entry) {
        super(time, totalCount, entry);
        _distinctCount = distinctCount;
    }

    public long getDistinctCount() {
        return _distinctCount;
    }

}
