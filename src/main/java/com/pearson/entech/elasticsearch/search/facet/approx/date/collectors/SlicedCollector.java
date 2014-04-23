package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.trove.map.hash.TObjectLongHashMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalSlicedFacet;

/**
 * A Collector for sliced date facets.
 * 
 * @param <V> the field data type of the optional value field (use NullFieldData if you aren't using the value field)
 * @param <S> the field data type of the slice field
 */
public class SlicedCollector<V extends AtomicFieldData<? extends ScriptDocValues>, S extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    /**
     * Field data for the slice field.
     */
    private final IndexFieldData<S> _sliceFieldData;

    /**
     * Field data values for the slice field.
     */
    private BytesValues _sliceFieldValues;

    /**
     * A nested map from timestamps to slice labels to counts.  
     */
    private ExtTLongObjectHashMap<TObjectLongHashMap<BytesRef>> _counts;

    /**
     * Create a new Collector.
     * 
     * @param keyFieldData the key field (datetime) data
     * @param valueFieldData the value field data
     * @param sliceFieldData the distinct field data
     * @param tzRounding the timezone rounding to apply
     */
    public SlicedCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<V> valueFieldData,
            final IndexFieldData<S> sliceFieldData,
            final TimeZoneRounding tzRounding) {
        super(keyFieldData, valueFieldData, tzRounding);
        _sliceFieldData = sliceFieldData;
        _counts = CacheRecycler.popLongObjectMap();
    }

    /**
     * Create a new Collector.
     * 
     * @param keyFieldData the key field (datetime) data
     * @param sliceFieldData the distinct field data
     * @param tzRounding the timezone rounding to apply
     */
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

    /**
     * Retrieve a slice labels->count map by timestamp, creating it if it doesn't exist already,
     * and increment the count for a given slice label.
     * 
     * @param counts the timestamp->slice label->count map
     * @param key the timestamp required
     * @param unsafe a BytesRef holding the newly-seen slice label -- this will be made safe automatically
     */
    private void incrementSafely(final TLongObjectMap<TObjectLongHashMap<BytesRef>> counts,
            final long key, final BytesRef unsafe) {
        TObjectLongHashMap<BytesRef> subMap = counts.get(key);
        if(subMap == null) {
            subMap = new TObjectLongHashMap<BytesRef>(); // no CacheRecycler for these
            counts.put(key, subMap);
        }
        final BytesRef safe = BytesRef.deepCopyOf(unsafe);
        subMap.adjustOrPutValue(safe, 1, 1);
    }

    /**
     * Get an iterator over slice field values found in a given doc.
     * 
     * @param doc the doc ID
     * @return the iterator
     */
    private BytesValues.Iter getSliceIter(final int doc) {
        final BytesValues.Iter sliceIter =
                _sliceFieldValues.getIter(doc);
        return sliceIter;
    }

}
