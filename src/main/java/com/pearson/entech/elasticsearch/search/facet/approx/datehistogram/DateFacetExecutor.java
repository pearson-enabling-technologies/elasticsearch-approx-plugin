package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.TIntObjectMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.trove.map.hash.TIntIntHashMap;
import org.elasticsearch.common.trove.map.hash.TIntObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.trove.procedure.TObjectIntProcedure;
import org.elasticsearch.common.trove.procedure.TObjectObjectProcedure;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.LongValues.WithOrdinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

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

    // TODO remove all printlns
    // TODO calculate for-loop boundary values before starting loops (not each time)
    // TODO better checking for 0-length collections and other trip-ups
    // TODO sorting of data within facets
    // TODO complete tests (see notes in files)
    // TODO keep track of missing values
    // TODO replace "new DistinctCountPayload()" with an object cache
    // TODO global cache of the _counts from each collector
    // TODO limits on terms used in slicing (min freq/top N)
    // TODO make interval optional, so we can just have one bucket (custom TimeZoneRounding)
    // TODO stop using long arrays as wrappers for counters (materialize methods)
    // TODO support other slice labels apart from String?
    // TODO replace NullEntry with a mixin for having an entry, maybe
    // TODO surface the slice field and the distinct field name in the results
    // TODO exclude deserialized and other "foreign" objects from CacheRecycler
    // TODO better Java API (don't use internal classes)
    // TODO init() for ordinal->term procedures at end of each collector, to pass in data structures and avoid sticky references
    // TODO make these collectors static classes, or break them out (to avoid ref. to executor)
    // TODO wrappers around iterators so we can get bytes for numeric fields without converting to strings first

    private class CountingCollector extends BuildableCollector {

        private BytesValues _valueFieldValues;

        private final TIntIntHashMap _countsByOrdinal;
        private final TLongIntHashMap _counts;

        CountingCollector() {
            _countsByOrdinal = CacheRecycler.popIntIntMap();
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
                while(hasNextOrdinal()) {
                    _countsByOrdinal.adjustOrPutValue(nextOrdinal(), 1, 1);
                }
            } else {
                while(hasNextOrdinal()) {
                    // We are counting each occurrence of valueField (regardless of its contents)
                    final org.elasticsearch.index.fielddata.BytesValues.Iter valIter =
                            _valueFieldValues.getIter(doc);
                    if(!valIter.hasNext())
                        return;

                    while(valIter.hasNext()) {
                        valIter.next();
                        _countsByOrdinal.adjustOrPutValue(nextOrdinal(), 1, 1);
                    }
                }
            }
        }

        @Override
        protected void postReader() {
            _valueFieldValues = null;

            final int uniqueOrds = ordinalCount();
            for(int i = 0; i < uniqueOrds; i++) { // 1 or 0?!?
                if(_countsByOrdinal.containsKey(i)) {
                    final int count = _countsByOrdinal.get(i);
                    _counts.adjustOrPutValue(getRoundedTimestamp(i), count, count);
                }
            }

            _countsByOrdinal.clear();
        }

        @Override
        public InternalFacet build(final String facetName) {
            final InternalFacet facet = new InternalCountingFacet(facetName, _counts);
            CacheRecycler.pushIntIntMap(_countsByOrdinal);
            return facet;
        }

    }

    private class SlicedCollector extends BuildableCollector {

        private BytesValues _sliceFieldValues;
        private BytesValues _valueFieldValues;

        private final TIntObjectHashMap<TObjectIntHashMap<BytesRef>> _countsByOrdinal;
        private final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> _counts;

        SlicedCollector() {
            _countsByOrdinal = CacheRecycler.popIntObjectMap();
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
            if(!hasNextOrdinal())
                return;

            if(_valueFieldData == null) {
                // We are only counting docs for each slice
                while(hasNextOrdinal()) {
                    final org.elasticsearch.index.fielddata.BytesValues.Iter sliceIter =
                            _sliceFieldValues.getIter(doc);
                    if(!sliceIter.hasNext())
                        return;

                    while(sliceIter.hasNext()) {
                        // TODO we can reduce hash lookups by getting the outer map in the outer loop
                        incrementSafely(_countsByOrdinal, nextOrdinal(), sliceIter.next());
                    }
                }
            } else {
                // We are counting each occurrence of value_field in each slice (regardless of its contents)
                while(hasNextOrdinal()) {
                    final org.elasticsearch.index.fielddata.BytesValues.Iter sliceIter =
                            _sliceFieldValues.getIter(doc);
                    if(!sliceIter.hasNext())
                        return;

                    while(sliceIter.hasNext()) {
                        final org.elasticsearch.index.fielddata.BytesValues.Iter valIter =
                                _valueFieldValues.getIter(doc);
                        while(valIter.hasNext()) {
                            // TODO we can reduce hash lookups by getting the outer map in the outer loop
                            final BytesRef unsafe = sliceIter.next();
                            incrementSafely(_countsByOrdinal, nextOrdinal(), unsafe);
                        }
                    }
                }
            }

        }

        @Override
        public void postReader() {
            _valueFieldValues = null;
            _sliceFieldValues = null;
            // Count from 1 as 0 = missing
            final int uniqueOrds = ordinalCount();
            for(int i = 1; i < uniqueOrds; i++) {
                if(_countsByOrdinal.containsKey(i)) {
                    final long roundedTimestamp = getRoundedTimestamp(i);
                    final TObjectIntHashMap<BytesRef> sourcePeriod = _countsByOrdinal.get(i);
                    if(_counts.containsKey(roundedTimestamp)) {
                        final TObjectIntHashMap<BytesRef> destPeriod = _counts.get(roundedTimestamp);
                        sourcePeriod.forEachEntry(new TObjectIntProcedure<BytesRef>() {
                            @Override
                            public boolean execute(final BytesRef sliceLabel, final int sliceCount) {
                                destPeriod.adjustOrPutValue(sliceLabel, sliceCount, sliceCount);
                                return true;
                            }
                        });
                    } else {
                        _counts.put(roundedTimestamp, sourcePeriod);
                    }
                }
            }
            _countsByOrdinal.clear();
        }

        @Override
        public InternalFacet build(final String facetName) {
            final InternalFacet facet = new InternalSlicedFacet(facetName, _counts);
            CacheRecycler.pushIntObjectMap(_countsByOrdinal);
            return facet;
        }

        private void incrementSafely(final TIntObjectMap<TObjectIntHashMap<BytesRef>> countsByOrdinal,
                final int key, final BytesRef unsafe) {
            TObjectIntHashMap<BytesRef> subMap = countsByOrdinal.get(key);
            if(subMap == null) {
                subMap = CacheRecycler.popObjectIntMap();
                countsByOrdinal.put(key, subMap);
            }
            final BytesRef safe = BytesRef.deepCopyOf(unsafe);
            subMap.adjustOrPutValue(safe, 1, 1);
        }

    }

    private class DistinctCollector extends BuildableCollector {

        private BytesValues _distinctFieldValues;

        private final TIntObjectHashMap<DistinctCountPayload> _countsByOrdinal;
        private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;

        DistinctCollector() {
            _countsByOrdinal = CacheRecycler.popIntObjectMap();
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
            if(!hasNextOrdinal())
                return;

            final org.elasticsearch.index.fielddata.BytesValues.Iter distinctIter =
                    _distinctFieldValues.getIter(doc);
            if(!distinctIter.hasNext())
                return;

            while(hasNextOrdinal()) {
                final DistinctCountPayload count = getSafely(_countsByOrdinal, nextOrdinal());
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
        public void postReader() {
            _distinctFieldValues = null;
            // Count from 1 as 0 = missing
            for(int i = 1; i < ordinalCount(); i++) {
                if(_countsByOrdinal.containsKey(i)) {
                    final DistinctCountPayload payload = _countsByOrdinal.get(i);
                    final long roundedTimestamp = getRoundedTimestamp(i);
                    if(_counts.containsKey(roundedTimestamp)) {
                        try {
                            _counts.get(roundedTimestamp).merge(payload);
                        } catch(final CardinalityMergeException e) {
                            throw new IllegalArgumentException(e);
                        }
                    } else {
                        _counts.put(roundedTimestamp, payload);
                    }
                }
            }
            _countsByOrdinal.clear();
        }

        @Override
        public InternalFacet build(final String facetName) {
            final InternalFacet facet = new InternalDistinctFacet(facetName, _counts);
            CacheRecycler.pushIntObjectMap(_countsByOrdinal);
            return facet;
        }

        private DistinctCountPayload getSafely(final TIntObjectMap<DistinctCountPayload> counts, final int key) {
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

        private final TIntObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _countsByOrdinal;
        private final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

        SlicedDistinctCollector() {
            _countsByOrdinal = CacheRecycler.popIntObjectMap();
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
            if(!hasNextOrdinal())
                return;

            final org.elasticsearch.index.fielddata.BytesValues.Iter distinctIter =
                    _distinctFieldValues.getIter(doc);
            final org.elasticsearch.index.fielddata.BytesValues.Iter sliceIter =
                    _sliceFieldValues.getIter(doc);

            while(hasNextOrdinal()) {
                while(sliceIter.hasNext()) {
                    // TODO we can reduce hash lookups by getting the outer map in the outer loop
                    final BytesRef unsafeSlice = sliceIter.next();
                    final DistinctCountPayload count = getSafely(_counts, nextOrdinal(), unsafeSlice);
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
        public void postReader() {
            _distinctFieldValues = null;
            _sliceFieldValues = null;
            // Count from 1 as 0 = missing
            final int uniqueOrds = ordinalCount();
            for(int i = 1; i < uniqueOrds; i++) {
                if(_countsByOrdinal.containsKey(i)) {
                    final long roundedTimestamp = getRoundedTimestamp(i);
                    final ExtTHashMap<BytesRef, DistinctCountPayload> sourcePeriod = _countsByOrdinal.get(i);
                    if(_counts.containsKey(roundedTimestamp)) {
                        final ExtTHashMap<BytesRef, DistinctCountPayload> destPeriod = _counts.get(roundedTimestamp);
                        sourcePeriod.forEachEntry(new TObjectObjectProcedure<BytesRef, DistinctCountPayload>() {
                            @Override
                            public boolean execute(final BytesRef sliceLabel, final DistinctCountPayload payload) {
                                if(destPeriod.containsKey(sliceLabel))
                                    try {
                                        destPeriod.get(sliceLabel).merge(payload);
                                    } catch(final CardinalityMergeException e) {
                                        throw new IllegalArgumentException(e);
                                    }
                                else
                                    destPeriod.put(sliceLabel, payload);
                                return true;
                            }
                        });
                    } else {
                        _counts.put(roundedTimestamp, sourcePeriod);
                    }
                }
            }
            _countsByOrdinal.clear();
        }

        @Override
        public InternalFacet build(final String facetName) {
            final InternalFacet facet = new InternalSlicedDistinctFacet(facetName, _counts);
            CacheRecycler.pushIntObjectMap(_countsByOrdinal);
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
        private Docs _ordinals;
        private IntsRef _docOrdinals;
        private int _docOrdinalsLength;
        private int _docOrdinalsOffset;
        private int _docOrdinalPointer;
        private long[] _roundedTimestamps;

        private int _maxOrdinal = -1;

        protected int nextOrdinal() {
            int ordinal;
            if(hasNextOrdinal()) {
                ordinal = _docOrdinals.ints[_docOrdinalPointer];
                if(ordinal > _maxOrdinal)
                    _maxOrdinal = ordinal;
                _docOrdinalPointer++;
            } else {
                throw new IllegalStateException("nextOrdinal() called when no more ordinals available");
            }
            return ordinal;
        }

        protected boolean hasNextOrdinal() {
            return _docOrdinalsLength > 0 &&
                    _docOrdinalPointer < _docOrdinalsLength + _docOrdinalsOffset;
        }

        protected long getRoundedTimestamp(final int ordinal) {
            return _roundedTimestamps[ordinal];
        }

        protected int ordinalCount() {
            return _roundedTimestamps.length;
        }

        @Override
        public void collect(final int doc) throws IOException {
            _docOrdinals = _ordinals.getOrds(doc);
            _docOrdinalsLength = _docOrdinals.length;
            _docOrdinalsOffset = _docOrdinals.offset;
            _docOrdinalPointer = _docOrdinalsOffset;
        }

        abstract InternalFacet build(String facetName);

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            if(_maxOrdinal != -1) {
                _roundedTimestamps = new long[_maxOrdinal + 1];
                // Start at 1 because ordinal 0 represents "no value"
                for(int i = 1; i < _roundedTimestamps.length; i++) {
                    _roundedTimestamps[i] = _tzRounding.calc(_keyFieldValues.getValueByOrd(i));
                }
                postReader();
            }

            _maxOrdinal = -1;
            _keyFieldValues = (WithOrdinals) ((LongArrayIndexFieldData) _keyFieldData.data)
                    .load(context).getLongValues();
            //            _docBase = context.docBase;
            _ordinals = _keyFieldValues.ordinals();
        }

        protected abstract void postReader();

        @Override
        public void postCollection() {
            postReader();
            _keyFieldValues = null;
            _docOrdinals = null;
            _roundedTimestamps = null;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return true;
        }

    }

}
