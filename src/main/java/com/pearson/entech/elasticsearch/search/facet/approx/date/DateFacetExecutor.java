package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.common.trove.list.array.TLongArrayList;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.LongValues.Iter;
import org.elasticsearch.index.fielddata.LongValues.WithOrdinals;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

public class DateFacetExecutor extends FacetExecutor {

    private final TypedFieldData _keyFieldData;
    private final TypedFieldData _valueFieldData;
    private final TypedFieldData _distinctFieldData;
    private final TypedFieldData _sliceFieldData;

    private final BuildableCollector _collector;

    private final TimeZoneRounding _tzRounding;

    private final int _exactThreshold;

    private final boolean _debug;
    private long _debugTotalCount;
    private long _debugDistinctCount;
    private DistinctCountPayload _debugCurrPayload;

    public DateFacetExecutor(final TypedFieldData keyFieldData, final TypedFieldData valueFieldData,
            final TypedFieldData distinctFieldData, final TypedFieldData sliceFieldData,
            final TimeZoneRounding tzRounding, final int exactThreshold, final boolean debug) {
        _keyFieldData = keyFieldData;
        _valueFieldData = valueFieldData;
        _distinctFieldData = distinctFieldData;
        _sliceFieldData = sliceFieldData;
        _tzRounding = tzRounding;
        _exactThreshold = exactThreshold;
        if(_distinctFieldData == null && _sliceFieldData == null)
            _collector = new CountingCollector();
        else if(_distinctFieldData == null)
            _collector = new SlicedCollector();
        else if(_sliceFieldData == null)
            _collector = new DistinctCollector();
        else
            _collector = new SlicedDistinctCollector();
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

    // TODO global cache of the counts from each collector
    // TODO limits on terms used in slicing (min freq/top N)
    // TODO make interval optional, so we can just have one bucket (custom TimeZoneRounding)
    // TODO stop using long arrays as wrappers for counters (materialize methods)
    // TODO support other slice labels apart from String?
    // TODO replace NullEntry with a mixin for having an entry, maybe
    // TODO surface the slice field and the distinct field name in the results
    // TODO exclude deserialized and other "foreign" objects from CacheRecycler
    // TODO better Java API (don't use internal classes)
    // TODO make these collectors static classes, or break them out (to avoid ref. to executor)
    // TODO wrappers around iterators so we can get bytes for numeric fields without converting to strings first

    private class CountingCollector extends BuildableCollector {

        private BytesValues _valueFieldValues;

        private TLongIntHashMap _counts;

        CountingCollector() {
            _counts = CacheRecycler.popLongIntMap();
        }

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            if(_valueFieldData != null)
                _valueFieldValues = _valueFieldData.data.load(context).getBytesValues();
        }

        @Override
        public void collect(final int doc) throws IOException {
            super.collect(doc);

            if(_valueFieldData == null) {
                // We are only counting docs
                while(hasNextTimestamp()) {
                    final long time = nextTimestamp();
                    _counts.adjustOrPutValue(time, 1, 1);
                }
            } else {
                while(hasNextTimestamp()) {
                    // We are counting each occurrence of valueField (regardless of its contents)
                    final org.elasticsearch.index.fielddata.BytesValues.Iter valIter =
                            _valueFieldValues.getIter(doc);
                    if(!valIter.hasNext())
                        return;

                    final long time = nextTimestamp();
                    while(valIter.hasNext()) {
                        valIter.next();
                        _counts.adjustOrPutValue(time, 1, 1);
                    }
                }
            }
        }

        @Override
        public InternalFacet build(final String facetName) {
            final InternalFacet facet = new InternalCountingFacet(facetName, _counts);
            _counts = null;
            return facet;
        }

    }

    private class SlicedCollector extends BuildableCollector {

        private BytesValues _sliceFieldValues;
        private BytesValues _valueFieldValues;

        private ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> _counts;

        SlicedCollector() {
            _counts = CacheRecycler.popLongObjectMap();
        }

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            _sliceFieldValues = _sliceFieldData.data.load(context).getBytesValues();
            if(_valueFieldData != null)
                _valueFieldValues = _valueFieldData.data.load(context).getBytesValues();
        }

        @Override
        public void collect(final int doc) throws IOException {
            // Exit as early as possible in order to avoid unnecessary lookups
            super.collect(doc);
            if(!hasNextTimestamp())
                return;

            if(_valueFieldData == null) {
                // We are only counting docs for each slice
                while(hasNextTimestamp()) {
                    final org.elasticsearch.index.fielddata.BytesValues.Iter sliceIter =
                            _sliceFieldValues.getIter(doc);
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
                    final org.elasticsearch.index.fielddata.BytesValues.Iter sliceIter =
                            _sliceFieldValues.getIter(doc);
                    if(!sliceIter.hasNext())
                        return;

                    final long time = nextTimestamp();

                    while(sliceIter.hasNext()) {
                        final org.elasticsearch.index.fielddata.BytesValues.Iter valIter =
                                _valueFieldValues.getIter(doc);
                        while(valIter.hasNext()) {
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

    }

    private class DistinctCollector extends BuildableCollector {

        private BytesValues _distinctFieldValues;

        private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;

        DistinctCollector() {
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

    private class SlicedDistinctCollector extends BuildableCollector {

        private BytesValues _distinctFieldValues;
        private BytesValues _sliceFieldValues;

        private final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

        SlicedDistinctCollector() {
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

    // TODO really this needs to be two classes, for ordinal and non-ordinal data
    private abstract class BuildableCollector extends Collector {

        private LongValues _keyFieldValues;
        private IntsRef _docOrds;
        private int _docOrdPointer;
        private final TLongArrayList _timestamps = new TLongArrayList();
        private final TIntArrayList _ordToTimestampPointers = new TIntArrayList();
        private Iter _docIter;
        private final Iter _emptyIter = new Iter.Empty(); // TODO static

        private long _lastNonOrdDatetime = 0;
        private long _lastNonOrdTimestamp = 0;

        protected long nextTimestamp() {
            if(_keyFieldValues instanceof WithOrdinals) {
                final long ts = _timestamps.get(_ordToTimestampPointers.get(_docOrds.ints[_docOrdPointer]));
                _docOrdPointer++;
                return ts;
            } else {
                final long datetime = _docIter.next();
                // If this datetime is less than a second after the previously-seen timestamp, it will have the same timestamp
                // (true because we don't support granularity less than 1 sec)
                if(datetime == _lastNonOrdDatetime || (datetime > _lastNonOrdTimestamp && datetime - _lastNonOrdTimestamp < 1000)) {
                    _lastNonOrdDatetime = datetime;
                    // _lastNonOrdTimestamp already contains right value
                } else {
                    // Get and save new timestamp
                    _lastNonOrdDatetime = datetime;
                    _lastNonOrdTimestamp = _tzRounding.calc(datetime);
                }
                return _lastNonOrdTimestamp;
            }
        }

        protected boolean hasNextTimestamp() {
            if(_keyFieldValues instanceof WithOrdinals) {
                return _docOrdPointer < _docOrds.length;
            } else {
                return _docIter.hasNext();
            }
        }

        @Override
        public void collect(final int doc) throws IOException {
            if(_keyFieldValues instanceof WithOrdinals) {
                _docOrds = ((WithOrdinals) _keyFieldValues).ordinals().getOrds(doc);
                _docOrdPointer = _docOrds.offset;
            } else {
                _docIter = _keyFieldValues.getIter(doc);
            }
        }

        abstract InternalFacet build(String facetName);

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            _keyFieldValues = ((LongArrayIndexFieldData) _keyFieldData.data)
                    .load(context).getLongValues();
            if(_keyFieldValues instanceof WithOrdinals) {
                final int maxOrd = ((WithOrdinals) _keyFieldValues).ordinals().getMaxOrd();
                int tsPointer = 0;
                _timestamps.resetQuick();
                _timestamps.add(0);
                _ordToTimestampPointers.resetQuick();
                _ordToTimestampPointers.add(0);
                long lastDateTime = 0;
                long lastTimestamp = 0;
                // TODO cache these lookup tables
                for(int i = 1; i < maxOrd; i++) {
                    final long datetime = ((WithOrdinals) _keyFieldValues).getValueByOrd(i);

                    // If this datetime is less than a second after the previously-seen timestamp, it will have the same timestamp
                    // (true because we don't support granularity less than 1 sec)
                    if(datetime == lastDateTime || (datetime > lastTimestamp && datetime - lastTimestamp < 1000)) {
                        // Just add another instance of the same timestamp pointer
                        _ordToTimestampPointers.add(tsPointer);
                    } else {
                        // We may or may not have a new timestamp
                        final long newTimestamp = _tzRounding.calc(datetime);
                        if(newTimestamp != lastTimestamp) {
                            // We do -- save it and update pointer
                            lastTimestamp = newTimestamp;
                            _timestamps.add(newTimestamp);
                            tsPointer++;
                            // Otherwise this ord will have the same pointer as the last one
                        }
                    }
                    lastDateTime = datetime;
                    _ordToTimestampPointers.add(tsPointer);
                }
            } else {
                _docIter = _emptyIter;
            }
        }

        @Override
        public void postCollection() {}

    }

}
