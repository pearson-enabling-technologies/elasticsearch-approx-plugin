package com.pearson.entech.elasticsearch.search.facet.approx.date;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.search.facet.Facet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

public class InternalSlicedDistinctFacet
        extends DateFacet<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>>
        implements HasDistinct {

    private ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

    private long _total;
    private List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> _periods;
    private long _distinctCount;

    private static final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> EMPTY = CacheRecycler.popLongObjectMap();
    static final String TYPE = "sliced_distinct_date_facet";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    public static InternalSlicedDistinctFacet readHistogramFacet(final StreamInput in) throws IOException {
        final InternalSlicedDistinctFacet facet = new InternalSlicedDistinctFacet();
        facet.readFrom(in);
        return facet;
    }

    // Only for deserialization
    protected InternalSlicedDistinctFacet() {
        super("not set");
    }

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
    public long getTotalCount() {
        materialize();
        return _total;
    }

    @Override
    public List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> getTimePeriods() {
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

    @SuppressWarnings("unchecked")
    @Override
    ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> peekCounts() {
        return _counts;
    }

    @Override
    protected void readData(final StreamInput in) throws IOException {
        _counts = CacheRecycler.popLongObjectMap();
        final int size = in.readVInt();
        for(int i = 0; i < size; i++) {
            final long key = in.readVLong();
            final int sliceCount = in.readVInt();
            final ExtTHashMap<BytesRef, DistinctCountPayload> slice = CacheRecycler.popHashMap();
            for(int j = 0; j < sliceCount; j++) {
                final BytesRef sliceLabel = in.readBytesRef();
                final DistinctCountPayload payload = new DistinctCountPayload(in);
                slice.put(sliceLabel, payload);
            }
            _counts.put(key, slice);
        }
    }

    @Override
    protected void writeData(final StreamOutput out) throws IOException {
        if(_counts == null) {
            out.writeVInt(0);
            return;
        }
        final int size = _counts.size();
        _serializePeriods.init(out, size);
        _counts.forEachEntry(_serializePeriods);
        _serializePeriods.clear();
    }

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
        if(_periods != null)
            return;
        if(_counts == null || _counts.size() == 0) {
            _total = 0;
            _distinctCount = 0;
            _periods = newArrayListWithCapacity(0);
            return;
        }
        _periods = newArrayListWithCapacity(_counts.size());
        final long[] counter = { 0 };
        _materializePeriods.init(_periods);
        _counts.forEachEntry(_materializePeriods);
        _materializePeriods.clear();
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = counter[0];
        releaseCache();
    }

    @Override
    protected void releaseCache() {
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

        private List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> _target;
        private DistinctCountPayload _accumulator;

        public void init(final List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> _periods) {
            _target = _periods;
            _accumulator = null;
        }

        // Called once per time period
        @Override
        public boolean execute(final long time, final ExtTHashMap<BytesRef, DistinctCountPayload> period) {
            // First create _output buffer for the slices from this period
            final XContentEnabledList<DistinctSlice<String>> buffer =
                    new XContentEnabledList<DistinctSlice<String>>(period.size(), Constants.SLICES);
            // Then materialize the slices into it, creating period-wise subtotals as we go along
            _materializeSlices.init(buffer);
            period.forEachEntry(_materializeSlices);
            // Save materialization results, and period-wise subtotals
            final DistinctCountPayload periodAccumulator = _materializeSlices.getAccumulator();
            final long count = periodAccumulator.getCount();
            final long cardinality = periodAccumulator.getCardinality().cardinality();
            _target.add(
                    new DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>(
                            time, count, cardinality, buffer));

            // Save the first payload accumulator we receive, and merge the others into it
            if(_accumulator == null)
                _accumulator = periodAccumulator;
            else
                try {
                    _accumulator.merge(periodAccumulator);
                } catch(final CardinalityMergeException e) {
                    throw new IllegalStateException(e);
                }
            return true;
        }

        public void clear() {
            _target = null;
            _accumulator = null;
            _materializeSlices.clear();
        }

        private final SliceMaterializer _materializeSlices = new SliceMaterializer();

        private static class SliceMaterializer implements TObjectObjectProcedure<BytesRef, DistinctCountPayload> {

            private List<DistinctSlice<String>> _target;
            private DistinctCountPayload _accumulator;

            public void init(final List<DistinctSlice<String>> target) {
                _target = target;
                _accumulator = null;
            }

            public DistinctCountPayload getAccumulator() {
                return _accumulator;
            }

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef key, final DistinctCountPayload payload) {
                _target.add(new DistinctSlice<String>(key.utf8ToString(),
                        payload.getCount(), payload.getCardinality().cardinality()));

                // Save the first payload we receive, and merge the others into it
                if(_accumulator == null)
                    _accumulator = payload;
                else
                    try {
                        _accumulator.merge(payload);
                    } catch(final CardinalityMergeException e) {
                        throw new IllegalStateException(e);
                    }

                return true;
            }

            public void clear() {
                _target = null;
                _accumulator = null;
            }

        }

    }

    private final PeriodSerializer _serializePeriods = new PeriodSerializer();

    private static final class PeriodSerializer implements TLongObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {

        private StreamOutput _output;

        public void init(final StreamOutput output, final int size) throws IOException {
            _output = output;
            output.writeVInt(size);
        }

        // Called once per time period
        @Override
        public boolean execute(final long key, final ExtTHashMap<BytesRef, DistinctCountPayload> period) {
            try {
                _output.writeVLong(key);
                _serializeSlices.init(_output, period.size());
            } catch(final IOException e) {
                throw new IllegalStateException(e);
            }
            period.forEachEntry(_serializeSlices);
            return true;
        }

        public void clear() {
            _output = null;
            _serializeSlices.clear();
        }

        private final SliceSerializer _serializeSlices = new SliceSerializer();

        private static final class SliceSerializer implements TObjectObjectProcedure<BytesRef, DistinctCountPayload> {

            private StreamOutput _output;

            public void init(final StreamOutput output, final int size) throws IOException {
                _output = output;
                output.writeVInt(size);
            }

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef sliceLabel, final DistinctCountPayload payload) {
                try {
                    _output.writeBytesRef(sliceLabel);
                    payload.writeTo(_output);
                } catch(final IOException e) {
                    throw new IllegalStateException(e);
                }
                return true;
            }

            public void clear() {
                _output = null;
            }

        }

    }

}
