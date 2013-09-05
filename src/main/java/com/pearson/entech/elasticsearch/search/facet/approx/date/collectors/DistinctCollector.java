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
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.DistinctCountPayload;
import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalDistinctFacet;

public class DistinctCollector<V extends AtomicFieldData<? extends ScriptDocValues>, D extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    private final int _exactThreshold;

    //    private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;

    // TODO Can we use a cleverer data structure here? Try with a trie
    private Map<BytesRef, TIntArrayList> _occurrences;

    private final boolean _debug = false;
    private long _debugTotalCount;
    private long _debugDistinctCount;
    private DistinctCountPayload _debugCurrPayload;
    private final BytesFieldIterator<AtomicFieldData<? extends ScriptDocValues>> _distinctFieldIter;

    public DistinctCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<D> distinctFieldData,
            final TimeZoneRounding tzRounding,
            final int exactThreshold) {
        super(keyFieldData, tzRounding);
        _distinctFieldIter = new BytesFieldIterator(distinctFieldData); // TODO type safety?
        _exactThreshold = exactThreshold;
        //        _counts = CacheRecycler.popLongObjectMap();
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

        while(_distinctFieldIter.hasNext()) {
            // NB this causes two conversions if the field's numeric
            final BytesRef unsafe = _distinctFieldIter.next();
            TIntArrayList timestampList = _occurrences.get(unsafe);
            if(timestampList == null) {
                final BytesRef safe = BytesRef.deepCopyOf(unsafe);
                timestampList = new TIntArrayList();
                _occurrences.put(safe, timestampList);
            }

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
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        for(final BytesRef fieldVal : _occurrences.keySet()) {
            final TIntArrayList timestampList = _occurrences.get(fieldVal);
            final int timestampCount = timestampList.size();
            for(int i = 0; i < timestampCount; i++) {
                final long timestampSecs = timestampList.get(i);
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
        final InternalFacet facet = new InternalDistinctFacet(facetName, counts, _debug);
        return facet;
    }

    private DistinctCountPayload getSafely(final TLongObjectMap<DistinctCountPayload> counts, final long key) {
        DistinctCountPayload payload = counts.get(key);
        if(payload == null) {
            payload = new DistinctCountPayload(_exactThreshold);
            counts.put(key, payload);
        }

        if(_debug) {
            _debugCurrPayload = payload;
            _debugTotalCount = payload.getCount();
            _debugDistinctCount = payload.getCardinality().cardinality();
        }

        return payload;
    }

}
