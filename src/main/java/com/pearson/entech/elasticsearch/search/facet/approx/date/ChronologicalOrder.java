package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.util.Comparator;

public enum ChronologicalOrder implements Comparator<TimePeriod<?>> {

    INSTANCE;

    @Override
    public int compare(final TimePeriod<?> o1, final TimePeriod<?> o2) {
        if(o1 == null) {
            if(o2 == null) {
                return 0;
            }
            return 1;
        }
        if(o2 == null) {
            return -1;
        }
        return(o1.getTime() < o2.getTime() ? -1 :
                (o1.getTime() == o2.getTime() ? 0 : 1));
    }

}
