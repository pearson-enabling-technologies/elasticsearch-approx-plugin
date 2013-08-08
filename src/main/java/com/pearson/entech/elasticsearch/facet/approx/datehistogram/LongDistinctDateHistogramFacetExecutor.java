package com.pearson.entech.elasticsearch.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
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

    private final TimeZoneRounding tzRounding;
    private final DistinctDateHistogramFacet.ComparatorType comparatorType;
    final ExtTLongObjectHashMap<DistinctCountPayload> counts;
    private final int maxExactPerShard;

    public LongDistinctDateHistogramFacetExecutor(final IndexNumericFieldData keyIndexFieldData,
            final IndexNumericFieldData distinctIndexFieldData,
            final TimeZoneRounding tzRounding,
            final DistinctDateHistogramFacet.ComparatorType comparatorType,
            final int maxExactPerShard) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.distinctIndexFieldData = distinctIndexFieldData;
        this.counts = CacheRecycler.popLongObjectMap();
        this.tzRounding = tzRounding;
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
            this.histoProc = new DateHistogramProc(counts, tzRounding, maxExactPerShard);
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

        LongValues valueValues;
        private final int maxExactPerShard;
        private final TimeZoneRounding tzRounding;
        final ExtTLongObjectHashMap<DistinctCountPayload> counts;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(final ExtTLongObjectHashMap<DistinctCountPayload> counts,
                final TimeZoneRounding tzRounding,
                final int maxExactPerShard) {
            this.tzRounding = tzRounding;
            this.counts = counts;
            this.maxExactPerShard = maxExactPerShard;
        }

        @Override
        protected void onValue(final int docId, final long value) {
            final long time = tzRounding.calc(value);
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
