package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

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

    public DateFacetExecutor(final TypedFieldData keyFieldData, final TypedFieldData valueFieldData,
            final TypedFieldData distinctFieldData, final TypedFieldData sliceFieldData,
            final TimeZoneRounding tzRounding, final int exactThreshold) {
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
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        return _collector.build(facetName);
    }

    @Override
    public Collector collector() {
        return _collector;
    }

    // TODO proper support for floating point numbers in distinct/slice/value fields
    // TODO calculate for-loop boundary values before starting loops (not each time)
    // TODO better checking for 0-length collections and other trip-ups
    // TODO sorting of data within facets
    // TODO complete tests (see notes in files)
    // TODO keep track of missing values
    // TODO replace "new DistinctCountPayload()" with an object cache
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
                // TODO if these aren't strings, this isn't the most efficient way:
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
            // TODO if these aren't strings, this isn't the most efficient way:
            _sliceFieldValues = _sliceFieldData.data.load(context).getBytesValues();
            if(_valueFieldData != null)
                // TODO if these aren't strings, this isn't the most efficient way:
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
                        // TODO we can reduce hash lookups by getting the outer map in the outer loop
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
                            // TODO we can reduce hash lookups by getting the outer map in the outer loop
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
            // TODO if these aren't strings, this isn't the most efficient way:
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
                    count.update(unsafe);
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
            final InternalFacet facet = new InternalDistinctFacet(facetName, _counts);
            return facet;
        }

        private DistinctCountPayload getSafely(final TLongObjectMap<DistinctCountPayload> counts, final long key) {
            DistinctCountPayload payload = counts.get(key);
            if(payload == null) {
                payload = new DistinctCountPayload(_exactThreshold);
                counts.put(key, payload);
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
            // TODO if these aren't strings, this isn't the most efficient way:
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
                    // TODO we can reduce hash lookups by getting the outer map in the outer loop
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

    private abstract class BuildableCollector extends Collector {

        private LongValues.WithOrdinals _keyFieldValues;
        private IntsRef _docOrds;
        private int _docOrdPointer;
        private final TLongArrayList _timestamps = new TLongArrayList();
        private final TIntArrayList _ordToTimestampPointers = new TIntArrayList();

        protected long nextTimestamp() {
            final long next = _timestamps.get(_ordToTimestampPointers.get(_docOrds.ints[_docOrdPointer]));
            _docOrdPointer++;
            return next;
        }

        protected boolean hasNextTimestamp() {
            return _docOrdPointer < _docOrds.length;
        }

        @Override
        public void collect(final int doc) throws IOException {
            _docOrds = _keyFieldValues.ordinals().getOrds(doc);
            _docOrdPointer = _docOrds.offset;
        }

        abstract InternalFacet build(String facetName);

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            _keyFieldValues = (WithOrdinals) ((LongArrayIndexFieldData) _keyFieldData.data)
                    .load(context).getLongValues();
            final int maxOrd = _keyFieldValues.ordinals().getMaxOrd();
            _timestamps.resetQuick();
            _timestamps.add(0);
            _ordToTimestampPointers.resetQuick();
            _ordToTimestampPointers.add(0);
            long lastNewTS = 0;
            int tsPointer = 0;
            // TODO cache these lookup tables
            for(int i = 1; i < maxOrd; i++) {
                final long datetime = _keyFieldValues.getValueByOrd(i);
                if(datetime > lastNewTS && datetime - lastNewTS > 1000) {
                    final long nextTS = _tzRounding.calc(datetime);
                    if(nextTS != lastNewTS) {
                        tsPointer++;
                        _timestamps.add(nextTS);
                        lastNewTS = nextTS;
                    }
                }
                _ordToTimestampPointers.add(tsPointer);
            }
            //            System.out.println(Thread.currentThread().getName() + " > After setNextReader:");
            //            System.out.println(Thread.currentThread().getName() + " > _timestamps = " + _timestamps);
            //            System.out.println(Thread.currentThread().getName() + " > _ordToTimestampPointers = " + Arrays.toString(_ordToTimestampPointers));
        }

        @Override
        public void postCollection() {
            //            CacheRecycler.pushIntArray(_ordToTimestampPointers);
            //            _timestamps.resetQuick();
        }

    }

}
