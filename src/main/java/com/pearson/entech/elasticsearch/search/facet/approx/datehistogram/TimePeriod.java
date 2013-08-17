package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

public abstract class TimePeriod<E> implements Comparable<TimePeriod<E>> {

    abstract long getTime();

    abstract E getEntry();

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
