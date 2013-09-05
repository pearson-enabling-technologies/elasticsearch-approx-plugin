package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.DistinctCountPayload;
import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalSlicedDistinctFacet;

public class SlicedDistinctCollector<V extends AtomicFieldData<? extends ScriptDocValues>, S extends AtomicFieldData<? extends ScriptDocValues>, D extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    private final int _exactThreshold;

    private final IndexFieldData<S> _sliceFieldData;
    private final IndexFieldData<D> _distinctFieldData;

    private BytesValues _sliceFieldValues;
    private BytesValues _distinctFieldValues;

    private final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

    public SlicedDistinctCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<V> valueFieldData,
            final IndexFieldData<S> sliceFieldData,
            final IndexFieldData<D> distinctFieldData,
            final TimeZoneRounding tzRounding,
            final int exactThreshold) {
        super(keyFieldData, valueFieldData, tzRounding);
        _sliceFieldData = sliceFieldData;
        _distinctFieldData = distinctFieldData;
        _exactThreshold = exactThreshold;
        _counts = CacheRecycler.popLongObjectMap();
    }

    public SlicedDistinctCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<S> sliceFieldData,
            final IndexFieldData<D> distinctFieldData,
            final TimeZoneRounding tzRounding,
            final int exactThreshold) {
        super(keyFieldData, tzRounding);
        _sliceFieldData = sliceFieldData;
        _distinctFieldData = distinctFieldData;
        _exactThreshold = exactThreshold;
        _counts = CacheRecycler.popLongObjectMap();
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
        _distinctFieldValues = _distinctFieldData.load(context).getBytesValues();
        _sliceFieldValues = _sliceFieldData.load(context).getBytesValues();
    }

    @Override
    public void collect(final int doc) throws IOException {
        // Exit as early as possible in order to avoid unnecessary lookups
        super.collect(doc);
        if(!hasNextTimestamp())
            return;

        final org.elasticsearch.index.fielddata.BytesValues.Iter distinctIter =
                _distinctFieldValues.getIter(doc);
        final org.elasticsearch.index.fielddata.BytesValues.Iter sliceIter =
                _sliceFieldValues.getIter(doc);

        while(hasNextTimestamp()) {
            final long time = nextTimestamp();
            while(sliceIter.hasNext()) {
                final BytesRef unsafeSlice = sliceIter.next();
                final DistinctCountPayload count = getSafely(_counts, time, unsafeSlice);
                while(distinctIter.hasNext()) {
                    final BytesRef unsafeTerm = distinctIter.next();
                    // Unsafe because this may change; the counter needs to make
                    // it safe if it's going to keep hold of the bytes
                    count.update(unsafeTerm);
                }
            }
        }
    }

    @Override
    public void postCollection() {
        super.postCollection();
        _distinctFieldValues = null;
        _sliceFieldValues = null;
    }

    @Override
    public InternalFacet build(final String facetName) {
        final InternalFacet facet = new InternalSlicedDistinctFacet(facetName, _counts);
        return facet;
    }

    private DistinctCountPayload getSafely(
            final TLongObjectMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts,
            final long key, final BytesRef unsafe) {
        ExtTHashMap<BytesRef, DistinctCountPayload> subMap = counts.get(key);
        if(subMap == null) {
            subMap = CacheRecycler.popHashMap();
            counts.put(key, subMap);
        }
        DistinctCountPayload payload = subMap.get(unsafe);
        if(payload == null) {
            final BytesRef safe = BytesRef.deepCopyOf(unsafe);
            payload = new DistinctCountPayload(_exactThreshold);
            subMap.put(safe, payload);
        }
        return payload;
    }

}
