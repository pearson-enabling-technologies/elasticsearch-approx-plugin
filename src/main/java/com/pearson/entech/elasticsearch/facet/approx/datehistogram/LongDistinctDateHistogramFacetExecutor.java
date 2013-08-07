package com.pearson.entech.elasticsearch.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;

/**
 * Collect the distinct values per time interval.
 */
public class LongDistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData keyIndexFieldData;
    private final IndexNumericFieldData distinctIndexFieldData;

    private final MutableDateTime dateTime;
    private final long interval;
    private final DistinctDateHistogramFacet.ComparatorType comparatorType;
    final ExtTLongObjectHashMap<DistinctCountPayload> counts;
    private final int maxExactPerShard;

    public LongDistinctDateHistogramFacetExecutor(final IndexNumericFieldData keyIndexFieldData,
            final IndexNumericFieldData distinctIndexFieldData,
            final MutableDateTime dateTime, final long interval,
            final DistinctDateHistogramFacet.ComparatorType comparatorType,
            final int maxExactPerShard) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.distinctIndexFieldData = distinctIndexFieldData;
        this.counts = CacheRecycler.popLongObjectMap();
        this.dateTime = dateTime;
        this.interval = interval;
        this.maxExactPerShard = maxExactPerShard;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        final LongInternalDistinctDateHistogramFacet facet = new LongInternalDistinctDateHistogramFacet(facetName, comparatorType, counts, true);
        System.out.println("Built facet " + facet);
        return facet;
    }

    /*
     * Similar to the Collector from the ValueDateHistogramFacetExecutor
     *
     * Only difference is that dateTime and interval is passed to DateHistogramProc instead of tzRounding
     */
    class Collector extends FacetExecutor.Collector {

        private LongValues keyValues;
        private final DateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new DateHistogramProc(counts, dateTime, interval, maxExactPerShard);
        }

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            histoProc.valueValues = distinctIndexFieldData.load(context).getLongValues();
        }

        @Override
        public void collect(final int doc) throws IOException {
            histoProc.onDoc(doc, keyValues);
        }

        @Override
        public void postCollection() {}
    }

    /**
     * Collect the time intervals in value aggregators for each time interval found.
     * The value aggregator finally contains the facet entry.
     *
     *
     */
    // TODO remove duplication between this and StringDistinctDateHistogramFacetExecutor
    public static class DateHistogramProc extends LongFacetAggregatorBase {

        private int total;
        private int missing;
        LongValues valueValues;
        private final long interval;
        private final int maxExactPerShard;
        private final MutableDateTime dateTime;
        final ExtTLongObjectHashMap<DistinctCountPayload> counts;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(final ExtTLongObjectHashMap<DistinctCountPayload> counts,
                final MutableDateTime dateTime,
                final long interval,
                final int maxExactPerShard) {
            this.dateTime = dateTime;
            this.counts = counts;
            this.interval = interval;
            this.maxExactPerShard = maxExactPerShard;
        }

        /*
         * Extend the onDoc implementation of LongFacetAggregatorBase to pass a dateTime to onValue
         * to account for the interval and rounding that is set in the Parser
         */
        @Override
        public void onDoc(final int docId, final LongValues values) {
            if(values.hasValue(docId)) {
                final LongValues.Iter iter = values.getIter(docId);
                while(iter.hasNext()) {
                    dateTime.setMillis(iter.next());
                    //dateTime = new MutableDateTime(iter.next());
                    onValue(docId, dateTime);
                    total++;
                }
            } else {
                missing++;
            }
        }

        protected void onValue(final int docId, final MutableDateTime dateTime) {
            final long time = dateTime.getMillis();
            onValue(docId, time);
        }

        /*
         * for each time interval an entry is created in which the distinct values are aggregated
         */
        @Override
        protected void onValue(final int docId, long time) {
            if(interval != 1) {
                time = ((time / interval) * interval);
            }

            DistinctCountPayload count = counts.get(time);
            if(count == null) {
                count = new DistinctCountPayload(maxExactPerShard);
                counts.put(time, count);
            }
            valueAggregator.entry = count;
            valueAggregator.onDoc(docId, valueValues);
        }

        /*
         * aggregates the values in a set
         */
        public final static class ValueAggregator extends LongFacetAggregatorBase {

            DistinctCountPayload entry;

            @Override
            public void onValue(final int docId, final long value) {
                entry.update(value);
            }
        }
    }
}
