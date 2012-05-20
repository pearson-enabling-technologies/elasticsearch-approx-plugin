package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.Comparator;
import java.util.List;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.search.facet.Facet;

/**
 * An approximate date histogram facet.
 */
public interface DateHistogramFacet extends Facet, Iterable<DateHistogramFacet.Entry> {

    /**
     * The type of the filter facet.
     */
    public static final String TYPE = "date_histogram_approx";

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
        TOTAL((byte) 2, "total", new Comparator<Entry>() {

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
                return(o1.total() < o2.total() ? -1 : (o1.total() == o2.total() ? 0 : 1));
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
                return TOTAL;
            }
            throw new ElasticSearchIllegalArgumentException("No type argument match for histogram comparator [" + id + "]");
        }

        public static ComparatorType fromString(final String type) {
            if("time".equals(type)) {
                return TIME;
            } else if("count".equals(type)) {
                return COUNT;
            } else if("total".equals(type)) {
                return TOTAL;
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
         * The total count of values aggregated to compute the total.
         */
        long totalCount();

        /**
         * The total count of values aggregated to compute the total.
         */
        long getTotalCount();

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        double total();

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        double getTotal();

        /**
         * The mean of this facet interval.
         */
        double mean();

        /**
         * The mean of this facet interval.
         */
        double getMean();

        /**
         * The minimum value.
         */
        double min();

        /**
         * The minimum value.
         */
        double getMin();

        /**
         * The maximum value.
         */
        double max();

        /**
         * The maximum value.
         */
        double getMax();
    }
}