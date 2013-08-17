package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

public class InternalSlicedDistinctFacet extends TimeFacet<DistinctTimePeriod<List<DistinctSlice<String>>>> implements HasDistinct {

    private final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

    private long _total;
    private List<DistinctTimePeriod<List<DistinctSlice<String>>>> _periods;
    private long _distinctCount;

    private static final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> EMPTY = CacheRecycler.popLongObjectMap();
    private static final String TYPE = "SlicedDistinctDateHistogramFacet";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    public InternalSlicedDistinctFacet(final String facetName,
            final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts) {
        super(facetName);
        _counts = counts;
    }

    @Override
    public long getDistinctCount() {
        materialize();
        return _distinctCount;
    }

    @Override
    public long getTotal() {
        materialize();
        return _total;
    }

    @Override
    public List<DistinctTimePeriod<List<DistinctSlice<String>>>> getTimePeriods() {
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
        return null;
    }

    // TODO reduce and materialize logic is similar to InternalSlicedFacet -- factor out?

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            // Reduce into the first facet; we will release its _counts on rendering into XContent
            final InternalSlicedDistinctFacet target = (InternalSlicedDistinctFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalSlicedDistinctFacet source = (InternalSlicedDistinctFacet) facets.get(i);
                _mergePeriods.target = target;
                source._counts.forEachEntry(_mergePeriods);
                _mergePeriods.target = null; // Avoid risk of garbage leaks
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalSlicedDistinctFacet(getName(), EMPTY);
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
        _counts.forEachValue(_releaseCachedMaps);
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final CacheReleaser _releaseCachedMaps = new CacheReleaser();

    private static class CacheReleaser implements TObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {
        @Override
        public boolean execute(final ExtTHashMap<BytesRef, DistinctCountPayload> map) {
            CacheRecycler.pushHashMap(map);
            return true;
        }
    }

    private final TimePeriodMerger _mergePeriods = new TimePeriodMerger();

    private static final class TimePeriodMerger implements TLongObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {

        InternalSlicedDistinctFacet target;

        // Called once per period
        @Override
        public boolean execute(final long time, final ExtTHashMap<BytesRef, DistinctCountPayload> slices) {
            // Does this time period exist in the target facet?
            ExtTHashMap<BytesRef, DistinctCountPayload> targetPeriod = target._counts.get(time);
            // If not, then pull one from the object cache to use
            if(targetPeriod == null) {
                targetPeriod = CacheRecycler.popHashMap();
                target._counts.put(time, targetPeriod);
            }

            // Add or update all slices
            _mergeSlices.target = targetPeriod;
            slices.forEachEntry(_mergeSlices);
            _mergeSlices.target = null; // Avoid risk of garbage leaks
            return true;
        }

        private final SliceMerger _mergeSlices = new SliceMerger();

        private static final class SliceMerger implements TObjectObjectProcedure<BytesRef, DistinctCountPayload> {

            ExtTHashMap<BytesRef, DistinctCountPayload> target;

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef sliceLabel, final DistinctCountPayload payload) {
                payload.mergeInto(target, sliceLabel);
                return true;
            }

        }

    }

    private final PeriodMaterializer _materializePeriods = new PeriodMaterializer();

    private static final class PeriodMaterializer implements TLongObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {

        private List<TimePeriod<List<Slice<String>>>> _target;
        private List<DistinctCountPayload> _subtotals;
        private DistinctCountPayload _accumulator;

        public void init(final List<TimePeriod<List<Slice<String>>>> target, final List<DistinctCountPayload> subtotals) {
            _target = target;
            _subtotals = subtotals;
        }

        // Called once per time period
        @Override
        public boolean execute(final long time, final ExtTHashMap<BytesRef, DistinctCountPayload> period) {
            // First create output buffer for the slices from this period
            final List<Slice<String>> buffer = newArrayListWithCapacity(period.size());
            // Then materialize the slices into it, creating period-wise subtotals as we go along
            _materializeSlices.init(buffer);
            period.forEachEntry(_materializeSlices);
            // Save materialization results, and subtotals
            _target.add(new TimePeriod<List<Slice<String>>>(time, buffer));
            final DistinctCountPayload accumulator = _materializeSlices.getAccumulator();
            _subtotals.add(accumulator);

            // Save the first payload accumulator we receive, and merge the others into it
            if(_accumulator == null)
                _accumulator = accumulator;
            else
                try {
                    _accumulator.merge(accumulator);
                } catch(final CardinalityMergeException e) {
                    throw new IllegalStateException(e);
                }
            return true;
        }

        private final SliceMaterializer _materializeSlices = new SliceMaterializer();

        private static class SliceMaterializer implements TObjectObjectProcedure<BytesRef, DistinctCountPayload> {

            public void init(final List<Slice<String>> buffer) {
                // TODO Auto-generated method stub

            }

            public DistinctCountPayload getAccumulator() {
                // TODO Auto-generated method stub
                return null;
            }

        }

    }

}
