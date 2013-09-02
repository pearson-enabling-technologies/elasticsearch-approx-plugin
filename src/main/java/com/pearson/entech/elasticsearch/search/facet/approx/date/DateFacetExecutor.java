package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LongValues.Iter;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.CountingCollector;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.NullFieldData;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.SlicedCollector;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.TimestampFirstCollector;

public class DateFacetExecutor extends FacetExecutor {

    private static final Iter __emptyIter = new Iter.Empty();

    private final LongArrayIndexFieldData _keyFieldData;
    private final IndexFieldData<?> _valueFieldData;
    private final IndexFieldData<?> _distinctFieldData;
    private final IndexFieldData<?> _sliceFieldData;

    private final TimestampFirstCollector _collector;

    private final TimeZoneRounding _tzRounding;

    private final int _exactThreshold;

    public DateFacetExecutor(final LongArrayIndexFieldData keyFieldData, final IndexFieldData<?> valueFieldData,
            final IndexFieldData distinctFieldData, final IndexFieldData sliceFieldData,
            final TimeZoneRounding tzRounding, final int exactThreshold, final boolean debug) {
        _keyFieldData = keyFieldData;
        _valueFieldData = valueFieldData;
        _distinctFieldData = distinctFieldData;
        _sliceFieldData = sliceFieldData;
        _tzRounding = tzRounding;
        _exactThreshold = exactThreshold;
        if(_distinctFieldData == null && _sliceFieldData == null)
            if(_valueFieldData == null)
                _collector = new CountingCollector<NullFieldData>(keyFieldData, tzRounding);
            else
                _collector = new CountingCollector(keyFieldData, _valueFieldData, tzRounding);
        else if(_distinctFieldData == null)
            if(_valueFieldData == null)
                _collector = new SlicedCollector(keyFieldData, sliceFieldData, tzRounding);
            else
                _collector = new SlicedCollector(keyFieldData, valueFieldData, sliceFieldData, tzRounding);
        else if(_sliceFieldData == null)
            _collector = new DistinctCollector(keyFieldData, tzRounding);
        else
            _collector = new SlicedDistinctCollector(keyFieldData, tzRounding);
        _debug = debug;
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        return _collector.build(facetName);
    }

    @Override
    public Collector collector() {
        return _collector;
    }

    private class DistinctCollector extends TimestampFirstCollector {

        private BytesValues _distinctFieldValues;

        private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;

        public DistinctCollector(final TypedFieldData keyFieldData, final TimeZoneRounding tzRounding) {
            super(keyFieldData, tzRounding);
            _counts = CacheRecycler.popLongObjectMap();
        }

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            _distinctFieldValues = _distinctFieldData.data.load(context).getBytesValues();
        }

        @Override
        public void collect(final int doc) throws IOException {
            // Exit as early as possible in order to avoid unnecessary lookups
            super.collect(doc);
            if(!hasNextTimestamp())
                return;

            final org.elasticsearch.index.fielddata.BytesValues.Iter distinctIter =
                    _distinctFieldValues.getIter(doc);
            if(!distinctIter.hasNext())
                return;

            while(hasNextTimestamp()) {
                final long time = nextTimestamp();
                final DistinctCountPayload count = getSafely(_counts, time);
                while(distinctIter.hasNext()) {
                    // NB this causes two conversions if the field's numeric
                    final BytesRef unsafe = distinctIter.next();
                    // Unsafe because this may change; the counter needs to make
                    // it safe if it's going to keep hold of the bytes
                    final boolean modified = count.update(unsafe);

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

    private class SlicedDistinctCollector extends TimestampFirstCollector {

        private BytesValues _distinctFieldValues;
        private BytesValues _sliceFieldValues;

        private final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

        public SlicedDistinctCollector(final TypedFieldData keyFieldData, final TimeZoneRounding tzRounding) {
            super(keyFieldData, tzRounding);
            _counts = CacheRecycler.popLongObjectMap();
        }

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            _distinctFieldValues = _distinctFieldData.data.load(context).getBytesValues();
            _sliceFieldValues = _sliceFieldData.data.load(context).getBytesValues();
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
            final BytesRef safe = BytesRef.deepCopyOf(unsafe);
            DistinctCountPayload payload = subMap.get(safe);
            if(payload == null) {
                payload = new DistinctCountPayload(_exactThreshold);
                subMap.put(safe, payload);
            }
            return payload;
        }

    }

}
