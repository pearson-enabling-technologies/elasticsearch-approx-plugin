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

/**
 * A Collector for sliced distinct date facets.
 * 
 * @param <V> the field data type of the optional value field (use NullFieldData if you aren't using the value field)
 * @param <S> the field data type of the slice field
 * @param <D> the field data type of the distinct field
 */
public class SlicedDistinctCollector<V extends AtomicFieldData<? extends ScriptDocValues>, S extends AtomicFieldData<? extends ScriptDocValues>, D extends AtomicFieldData<? extends ScriptDocValues>>
        extends TimestampFirstCollector<V> {

    /**
     * The number of exact distinct field values to record before tipping into approximate counting.
     */
    private final int _exactThreshold;

    /**
     * Field data for the slice field.
     */
    private final IndexFieldData<S> _sliceFieldData;

    /**
     * Field data for the distinct field.
     */
    private final IndexFieldData<D> _distinctFieldData;

    /**
     * Field data values for the slice field.
     */
    private BytesValues _sliceFieldValues;

    /**
     * Field data values for the distinct field.
     */
    private BytesValues _distinctFieldValues;

    /**
     * A nested map from timestamps to slice labels to distinct counts.  
     */
    private final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

    /**
     * Create a new Collector.
     * 
     * @param keyFieldData the key field (datetime) data
     * @param sliceFieldData the distinct field data
     * @param distinctFieldData the distinct field data
     * @param tzRounding the timezone rounding to apply
     * @param exactThreshold The number of exact distinct field values to record before tipping into approximate counting
     */
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
                    // Unsafe because the BytesRef may be changed if we continue reading,
                    // but the counter only needs to read it once immediately, so that's OK
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

    /**
     * Retrieve a slice labels->distinct count map by timestamp,
     * creating it if it doesn't exist already,
     * and increment the counter for a given slice label.
     * 
     * @param counts the timestamp->slice label->distinct count map
     * @param key the timestamp required
     * @param unsafe a BytesRef holding the newly-seen slice label -- this will be made safe automatically
     */
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
