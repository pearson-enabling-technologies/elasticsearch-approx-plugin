package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;


public class TimePeriod<E> {

    private final long _time;
    private final E _entry;

    public TimePeriod(final long time, final E entry) {
        _time = time;
        _entry = entry;
    }

    public long getTime() {
        return _time;
    }

    public E getEntry() {
        return _entry;
    }

}
