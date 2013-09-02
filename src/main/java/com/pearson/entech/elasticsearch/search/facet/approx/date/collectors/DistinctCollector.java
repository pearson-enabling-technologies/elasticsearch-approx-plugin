package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.DistinctCountPayload;
import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalDistinctFacet;

public class DistinctCollector<V extends AtomicFieldData<? extends ScriptDocValues>, D extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    private final IndexFieldData<D> _distinctFieldData;
    private final int _exactThreshold;

    private BytesValues _distinctFieldValues;

    private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;

    private final boolean _debug = false;
    private long _debugTotalCount;
    private long _debugDistinctCount;
    private DistinctCountPayload _debugCurrPayload;

    public DistinctCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<D> distinctFieldData,
            final TimeZoneRounding tzRounding,
            final int exactThreshold) {
        super(keyFieldData, tzRounding);
        _distinctFieldData = distinctFieldData;
        _exactThreshold = exactThreshold;
        _counts = CacheRecycler.popLongObjectMap();
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
        _distinctFieldValues = _distinctFieldData.load(context).getBytesValues();
    }

    @Override
    public void collect(final int doc) throws IOException {
        // Exit as early as possible in order to avoid unnecessary lookups
        super.collect(doc);
        if(!hasNextTimestamp())
            return;

        // TODO change this to iterate over the ordinals of the distinct field,
        // so we only have to copy out the bytes ref once per unique value, not
        // once per document

        final org.elasticsearch.index.fielddata.BytesValues.Iter distinctIter =
                _distinctFieldValues.getIter(doc);

        while(distinctIter.hasNext()) {
            // NB this causes two conversions if the field's numeric
            final BytesRef unsafe = distinctIter.next();
            if(hasNextTimestamp()) {
                final BytesRef safe = BytesRef.deepCopyOf(unsafe);
                // Unsafe because this may change; the counter needs to make
                // it safe if it's going to keep hold of the bytes
                while(hasNextTimestamp()) {
                    final long time = nextTimestamp();
                    final DistinctCountPayload count = getSafely(_counts, time);
                    final boolean modified = count.updateSafe(safe);

                    if(_debug) {
                        _debugTotalCount++;
                        if(modified) {
                            _debugDistinctCount++;
                        }
                        assert _debugTotalCount == count.getCount();
                        assert _debugDistinctCount == count.getCardinality().cardinality();
                    }
                }
            }
        }
    }

    @Override
    public void postCollection() {
        super.postCollection();
        _distinctFieldValues = null;
    }

    @Override
    public InternalFacet build(final String facetName) {
        final InternalFacet facet = new InternalDistinctFacet(facetName, _counts, _debug);
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
