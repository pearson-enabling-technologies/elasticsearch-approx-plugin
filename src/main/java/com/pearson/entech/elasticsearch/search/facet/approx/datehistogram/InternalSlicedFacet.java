package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectIntProcedure;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;

public class InternalSlicedFacet extends TimeFacet<TimePeriod<List<Slice<String>>>> {

    private final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> _counts;

    private long _total;
    private List<TimePeriod<List<Slice<String>>>> _periods;

    private static final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> EMPTY = new ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>>();
    private static final String TYPE = "sliced_date_histogram";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    public InternalSlicedFacet(final String facetName, final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> counts) {
        super(facetName);
        _counts = counts;
    }

    @Override
    public long getTotalCount() {
        materialize();
        return _total;
    }

    @Override
    public List<TimePeriod<List<Slice<String>>>> getTimePeriods() {
        materialize();
        return _periods;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // TODO Auto-generated method stub
        releaseCache();
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            // Reduce into the first facet; we will release its _counts on rendering into XContent
            final InternalSlicedFacet target = (InternalSlicedFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalSlicedFacet source = (InternalSlicedFacet) facets.get(i);
                _mergePeriods.target = target;
                source._counts.forEachEntry(_mergePeriods);
                _mergePeriods.target = null; // Avoid risk of garbage leaks
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalSlicedFacet(getName(), EMPTY);
        }
    }

    private synchronized void materialize() {
        _periods = newArrayListWithCapacity(_periods.size());
        final long[] counter = { 0 };
        _materializePeriods.init(_periods, counter);
        _counts.forEachEntry(_materializePeriods);
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = counter[0];
        releaseCache();
    }

    private void releaseCache() {
        _counts.forEachValue(new TObjectProcedure<TObjectIntHashMap<BytesRef>>() {
            @Override
            public boolean execute(final TObjectIntHashMap<BytesRef> subMap) {
                CacheRecycler.pushObjectIntMap(subMap);
                return true;
            }
        });
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final PeriodMerger _mergePeriods = new PeriodMerger();

    private static final class PeriodMerger implements TLongObjectProcedure<TObjectIntHashMap<BytesRef>> {

        InternalSlicedFacet target;

        // Called once per period
        @Override
        public boolean execute(final long time, final TObjectIntHashMap<BytesRef> slices) {
            // Does this time period exist in the target facet?
            TObjectIntHashMap<BytesRef> targetPeriod = target._counts.get(time);
            // If not, then pull one from the object cache to use
            if(targetPeriod == null) {
                targetPeriod = CacheRecycler.popObjectIntMap();
                target._counts.put(time, targetPeriod);
            }

            // Add or update all slices in this period
            _mergeSlices.target = targetPeriod;
            slices.forEachEntry(_mergeSlices);
            _mergeSlices.target = null; // Reduce risk of garbage leaks
            return true;
        }

        private final SliceMerger _mergeSlices = new SliceMerger();

        private static final class SliceMerger implements TObjectIntProcedure<BytesRef> {

            TObjectIntHashMap<BytesRef> target;

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef sliceLabel, final int count) {
                // Add or update count for this slice label
                target.adjustOrPutValue(sliceLabel, count, count);
                return true;
            }

        }

    }

    private final PeriodMaterializer _materializePeriods = new PeriodMaterializer();

    private static final class PeriodMaterializer implements TLongObjectProcedure<TObjectIntHashMap<BytesRef>> {

        private List<TimePeriod<List<Slice<String>>>> _target;
        private long[] _counter;

        public void init(final List<TimePeriod<List<Slice<String>>>> target, final long[] counter) {
            _target = target;
            _counter = counter;
        }

        // Called once per time period
        @Override
        public boolean execute(final long time, final TObjectIntHashMap<BytesRef> period) {
            // First create output buffer for the slices from this period
            final List<Slice<String>> buffer = newArrayListWithCapacity(period.size());
            // Then materialize the slices into it
            _materializeSlices.init(buffer, _counter);
            period.forEachEntry(_materializeSlices);
            // Finally save results
            _target.add(new TimePeriod<List<Slice<String>>>(time, _counter[0], buffer));
            return true;
        }

        private final SliceMaterializer _materializeSlices = new SliceMaterializer();

        private static final class SliceMaterializer implements TObjectIntProcedure<BytesRef> {

            private List<Slice<String>> _target;
            private long[] _counter;

            public void init(final List<Slice<String>> target, final long[] counter) {
                _target = target;
                _counter = counter;
            }

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef key, final int count) {
                _target.add(new Slice<String>(key.utf8ToString(), count));
                _counter[0] = _counter[0] + count;
                return true;
            }

        }

    }

}
