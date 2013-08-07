package com.pearson.entech.elasticsearch.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator;

/**
 * Collect the distinct values per time interval.
 */
public class StringDistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final LongArrayIndexFieldData keyIndexFieldData;
    private final PagedBytesIndexFieldData distinctIndexFieldData;

    private final MutableDateTime dateTime;
    private final long interval;
    private final DistinctDateHistogramFacet.ComparatorType comparatorType;
    final ExtTLongObjectHashMap<DistinctCountPayload> counts;
    private final int maxExactPerShard;

    public StringDistinctDateHistogramFacetExecutor(final LongArrayIndexFieldData keyIndexFieldData,
            final PagedBytesIndexFieldData distinctIndexFieldData,
            final MutableDateTime dateTime,
            final long interval,
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
        return new StringInternalDistinctDateHistogramFacet(facetName, comparatorType, counts, true);
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
            histoProc.valueValues = distinctIndexFieldData.load(context).getBytesValues();
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
     */
    // TODO remove duplication between this and LongDistinctDateHistogramFacetExecutor
    public static class DateHistogramProc {

        private int total;
        private int missing;
        BytesValues.WithOrdinals valueValues;
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
         * Pass a dateTime to onValue to account for the interval and rounding that is set in the Parser
         */
        public void onDoc(final int docId, final LongValues values) {
            if(values.hasValue(docId)) {
                final LongValues.Iter iter = values.getIter(docId);
                while(iter.hasNext()) {
                    dateTime.setMillis(iter.next());
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

        public final int total() {
            return total;
        }

        public final int missing() {
            return missing;
        }

        /*
         * aggregates the values in a set
         */
        public final static class ValueAggregator extends HashedAggregator {

            DistinctCountPayload entry;

            @Override
            protected void onValue(final int docId, final BytesRef value, final int hashCode, final BytesValues values) {
                final String val = value.utf8ToString();
                entry.update(val);
            }
        }
    }
}
