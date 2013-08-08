package com.pearson.entech.elasticsearch.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator;

/**
 * Collect the distinct values per time interval.
 */
public class StringDistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final LongArrayIndexFieldData keyIndexFieldData;
    private final PagedBytesIndexFieldData distinctIndexFieldData;

    private final TimeZoneRounding tzRounding;
    private final DistinctDateHistogramFacet.ComparatorType comparatorType;
    private final ExtTLongObjectHashMap<DistinctCountPayload> counts;
    private final int maxExactPerShard;

    public StringDistinctDateHistogramFacetExecutor(final LongArrayIndexFieldData keyIndexFieldData,
            final PagedBytesIndexFieldData distinctIndexFieldData,
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
        final StringInternalDistinctDateHistogramFacet facet = new StringInternalDistinctDateHistogramFacet(facetName, comparatorType, counts, true);
        System.out.println("Built facet " + facet);
        return facet;
    }

    class Collector extends FacetExecutor.Collector {

        private LongValues keyValues;
        private final DateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new DateHistogramProc(counts, tzRounding, maxExactPerShard);
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
    public static class DateHistogramProc extends LongFacetAggregatorBase {

        BytesValues.WithOrdinals valueValues;
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
        public void onValue(final int docId, final long value) {
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
