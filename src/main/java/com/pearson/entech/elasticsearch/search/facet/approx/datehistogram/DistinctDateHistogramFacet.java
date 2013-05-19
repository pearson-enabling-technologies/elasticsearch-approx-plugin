package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.Comparator;
import java.util.List;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facet.Facet;

/**
 * An approximate distinct-values date histogram facet.
 */
public interface DistinctDateHistogramFacet extends Facet, Iterable<DistinctDateHistogramFacet.Entry> {

    /**
     * The type of the facet.
     */
    public static final String TYPE = "distinct_date_histogram";

    /**
     * An ordered list of histogram facet entries.
     */
    List<? extends Entry> entries();

    /**
     * An ordered list of histogram facet entries.
     */
    List<? extends Entry> getEntries();

    public static enum ComparatorType {
        TIME((byte) 0, "time", new Comparator<Entry>() {

            @Override
            public int compare(final Entry o1, final Entry o2) {
                // push nulls to the end
                if(o1 == null) {
                    if(o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if(o2 == null) {
                    return -1;
                }
                return(o1.time() < o2.time() ? -1 : (o1.time() == o2.time() ? 0 : 1));
            }
        }),
        COUNT((byte) 1, "count", new Comparator<Entry>() {

            @Override
            public int compare(final Entry o1, final Entry o2) {
                // push nulls to the end
                if(o1 == null) {
                    if(o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if(o2 == null) {
                    return -1;
                }
                return(o1.count() < o2.count() ? -1 : (o1.count() == o2.count() ? 0 : 1));
            }
        }),
        DISTINCT((byte) 2, "distinct", new Comparator<Entry>() {

            @Override
            public int compare(final Entry o1, final Entry o2) {
                // push nulls to the end
                if(o1 == null) {
                    if(o2 == null) {
                        return 0;
                    }
                    return 1;
                }
                if(o2 == null) {
                    return -1;
                }
                return(o1.distinctCount() < o2.distinctCount() ? -1 : (o1.distinctCount() == o2.distinctCount() ? 0 : 1));
            }
        });

        private final byte id;

        private final String description;

        private final Comparator<Entry> comparator;

        ComparatorType(final byte id, final String description, final Comparator<Entry> comparator) {
            this.id = id;
            this.description = description;
            this.comparator = comparator;
        }

        public byte id() {
            return this.id;
        }

        public String description() {
            return this.description;
        }

        public Comparator<Entry> comparator() {
            return comparator;
        }

        public static ComparatorType fromId(final byte id) {
            if(id == 0) {
                return TIME;
            } else if(id == 1) {
                return COUNT;
            } else if(id == 2) {
                return DISTINCT;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + id + "]");
        }

        public static ComparatorType fromString(final String type) {
            if("time".equals(type)) {
                return TIME;
            } else if("count".equals(type)) {
                return COUNT;
            } else if("distinct".equals(type)) {
                return DISTINCT;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + type + "]");
        }
    }

    public interface Entry {

        /**
         * The time bucket start (in milliseconds).
         */
        long time();

        /**
         * The time bucket start (in milliseconds).
         */
        long getTime();

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        long count();

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        long getCount();

        /**
         * The number of distinct values of the value field that fall within that key "range" or "interval".
         */
        long distinctCount();

        /**
         * The number of distinct values of the value field that fall within that key "range" or "interval".
         */
        long getDistinctCount();

        long getTotalCount();

        double getTotal();

        double getMean();

        double getMin();

        double getMax();

    }
}