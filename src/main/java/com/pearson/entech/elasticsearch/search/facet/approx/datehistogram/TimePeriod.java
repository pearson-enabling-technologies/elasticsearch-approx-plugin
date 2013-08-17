package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public class TimePeriod<E> implements Comparable<TimePeriod<E>> {

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

    @Override
    public int compareTo(final TimePeriod<E> o) {
        // push nulls to the end
        if(o == null) {
            return -1;
        }
        return(getTime() < o.getTime() ? -1 :
                (getTime() == o.getTime() ? 0 : 1));
    }

}
