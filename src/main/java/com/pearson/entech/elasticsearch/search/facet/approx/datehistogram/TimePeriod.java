package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class TimePeriod<E> {

    private final long _time;
    private final long _totalCount;
    private final E _entry;

    public TimePeriod(final long time, final long totalCount, final E entry) {
        _time = time;
        _totalCount = totalCount;
        _entry = entry;
    }

    public long getTime() {
        return _time;
    }

    public long getTotalCount() {
        return _totalCount;
    }

    public E getEntry() {
        return _entry;
    }

}
