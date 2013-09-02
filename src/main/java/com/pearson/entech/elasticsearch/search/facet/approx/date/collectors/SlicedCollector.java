package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.InternalSlicedFacet;

public class SlicedCollector<V extends AtomicFieldData<? extends ScriptDocValues>, S extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    private final IndexFieldData<S> _sliceFieldData;

    private BytesValues _sliceFieldValues;
    private BytesValues.Iter _valueFieldIter;

    private ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> _counts;

    public SlicedCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<V> valueFieldData,
            final IndexFieldData<S> sliceFieldData,
            final TimeZoneRounding tzRounding) {
        super(keyFieldData, valueFieldData, tzRounding);
        _sliceFieldData = sliceFieldData;
        _counts = CacheRecycler.popLongObjectMap();
    }

    public SlicedCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<S> sliceFieldData,
            final TimeZoneRounding tzRounding) {
        super(keyFieldData, tzRounding);
        _sliceFieldData = sliceFieldData;
        _counts = CacheRecycler.popLongObjectMap();
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
        _sliceFieldValues = _sliceFieldData.load(context).getBytesValues();
    }

    @Override
    public void collect(final int doc) throws IOException {
        // Exit as early as possible in order to avoid unnecessary lookups
        super.collect(doc);
        if(!hasNextTimestamp())
            return;

        if(!hasValueField()) {
            // We are only counting docs for each slice
            while(hasNextTimestamp()) {
                final BytesValues.Iter sliceIter = getSliceIter(doc);
                if(!sliceIter.hasNext())
                    return;

                final long time = nextTimestamp();

                while(sliceIter.hasNext()) {
                    incrementSafely(_counts, time, sliceIter.next());
                }
            }
        } else {
            // We are counting each occurrence of value_field in each slice (regardless of its contents)
            while(hasNextTimestamp()) {
                final BytesValues.Iter sliceIter = getSliceIter(doc);
                if(!sliceIter.hasNext())
                    return;

                final long time = nextTimestamp();

                while(sliceIter.hasNext()) {
                    while(hasNextValue()) {
                        final BytesRef unsafe = sliceIter.next();
                        incrementSafely(_counts, time, unsafe);
                    }
                }
            }
        }

    }

    @Override
    public void postCollection() {
        super.postCollection();
        _sliceFieldValues = null;
    }

    @Override
    public InternalFacet build(final String facetName) {
        final InternalFacet facet = new InternalSlicedFacet(facetName, _counts);
        _counts = null;
        return facet;
    }

    private void incrementSafely(final TLongObjectMap<TObjectIntHashMap<BytesRef>> counts,
            final long key, final BytesRef unsafe) {
        TObjectIntHashMap<BytesRef> subMap = counts.get(key);
        if(subMap == null) {
            subMap = CacheRecycler.popObjectIntMap();
            counts.put(key, subMap);
        }
        final BytesRef safe = BytesRef.deepCopyOf(unsafe);
        subMap.adjustOrPutValue(safe, 1, 1);
    }

    private BytesValues.Iter getSliceIter(final int doc) {
        final BytesValues.Iter sliceIter =
                _sliceFieldValues.getIter(doc);
        return sliceIter;
    }

}
