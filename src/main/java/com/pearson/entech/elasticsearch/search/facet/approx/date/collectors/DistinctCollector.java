package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import static com.google.common.collect.Maps.newHashMap;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.DistinctCountPayload;
import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalDistinctFacet;

/**
 * A Collector for distinct date facets.
 * 
 * @param <V> the field data type of the optional value field (use NullFieldData if you aren't using the value field)
 * @param <D> the field data type of the distinct field
 */
public class DistinctCollector<V extends AtomicFieldData<? extends ScriptDocValues>, D extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    /**
     * The number of exact distinct field values to record before tipping into approximate counting.
     */
    private final int _exactThreshold;

    /**
     * A map from distinct field values to lists of timestamps.
     */
    private Map<BytesRef, TIntArrayList> _occurrences;

    /**
     * Iterator over the values of the distinct field.
     */
    private final BytesFieldIterator<AtomicFieldData<? extends ScriptDocValues>> _distinctFieldIter;

    /**
     * Create a new collector.
     * 
     * @param keyFieldData the key field (datetime) data
     * @param distinctFieldData the distinct field data
     * @param tzRounding the timezone rounding to apply
     * @param exactThreshold The number of exact distinct field values to record before tipping into approximate counting
     */
    public DistinctCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<D> distinctFieldData,
            final TimeZoneRounding tzRounding,
            final int exactThreshold) {
        super(keyFieldData, tzRounding);
        _distinctFieldIter = new BytesFieldIterator(distinctFieldData); // TODO type safety?
        _exactThreshold = exactThreshold;
        _occurrences = newHashMap();
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
        _distinctFieldIter.setNextReader(context);
    }

    @Override
    public void collect(final int doc) throws IOException {
        _distinctFieldIter.collect(doc);

        // Exit as early as possible in order to avoid unnecessary lookups/conversions
        super.collect(doc);
        if(!hasNextTimestamp())
            return;

        // Strategy: compile a map from distinct field values to timestamps where those values occur.
        // Later, at build time, we invert this into a map from timestamps to DistinctCountPayloads.
        // This avoids having to read, copy and store multiple BytesRefs containing the same distinct field values.

        while(_distinctFieldIter.hasNext()) {
            // TODO this causes two conversions if the field's numeric
            final BytesRef unsafe = _distinctFieldIter.next();
            TIntArrayList timestampList = _occurrences.get(unsafe);
            if(timestampList == null) {
                final BytesRef safe = BytesRef.deepCopyOf(unsafe);
                timestampList = new TIntArrayList();
                _occurrences.put(safe, timestampList);
            }

            // To reduce memory usage, we store all timestamps at second resolution for now
            while(hasNextTimestamp()) {
                final long time = nextTimestamp();
                timestampList.add((int) (time / 1000));
            }

            // Reset timestamp iterator for this doc
            // TODO make this a standalone CollectableIterator like _distinctFieldIter
            super.collect(doc);
        }
    }

    @Override
    public void postCollection() {
        super.postCollection();
        _distinctFieldIter.postCollection();
    }

    @Override
    public InternalFacet build(final String facetName) {
        // This is where we invert the distinct value->timestamp map to build the actual facet object
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        for(final BytesRef fieldVal : _occurrences.keySet()) {
            final TIntArrayList timestampList = _occurrences.get(fieldVal);
            final int timestampCount = timestampList.size();
            for(int i = 0; i < timestampCount; i++) {
                final long timestampSecs = timestampList.get(i);
                // Convert back to milliseconds resolution
                final long timestamp = timestampSecs * 1000;
                DistinctCountPayload payload = counts.get(timestamp);
                if(payload == null) {
                    payload = new DistinctCountPayload(_exactThreshold);
                    counts.put(timestamp, payload);
                }
                payload.update(fieldVal);
            }
            _occurrences.put(fieldVal, null); // Free this up for GC immediately
        }

        _occurrences = null;
        final InternalFacet facet = new InternalDistinctFacet(facetName, counts);
        return facet;
    }

}
