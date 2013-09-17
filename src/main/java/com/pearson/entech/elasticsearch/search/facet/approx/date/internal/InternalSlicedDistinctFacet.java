package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

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
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.Constants;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DistinctSlice;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DistinctTimePeriod;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.HasDistinct;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.XContentEnabledList;

/**
 * The internal representation of a SlicedDistinctDateFacet, holding the reduce and
 * materialize logic. This is only available on the server side, or via
 * ElasticSearch's JVM transport -- i.e. it doesn't survive XContent serialization.
 */
public class InternalSlicedDistinctFacet
        extends DateFacet<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>>
        implements HasDistinct {

    /**
     * The facet type, as shown in the JSON returned.
     */
    static final String TYPE = "sliced_distinct_date_facet";

    /**
     * Stream handler for this facet type.
     */
    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            final InternalSlicedDistinctFacet facet = new InternalSlicedDistinctFacet();
            facet.readFrom(in);
            return facet;
        }
    };

    /**
     * Register the stream handler with ElasticSearch.
     */
    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    /**
     * The stream type, used internally by ElasticSearch.
     */
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    /**
     * An empty sliced distinct counts map, shared between instances.
     */
    private static final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> EMPTY =
            CacheRecycler.popLongObjectMap();

    /**
     * Map from timestamps to slice labels to distinct counts.
     */
    private ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> _counts;

    /**
     * Total count across all slices and time periods.
     */
    private long _total;

    /**
     * Total distinct count across all slices and time periods.
     */
    private long _distinctCount;

    /**
     * List of time periods, only created in the materialize phase.
     */
    private List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> _periods;

    /**
     * Empty constructor for deserialization only -- do not use.
     */
    protected InternalSlicedDistinctFacet() {
        super("not set");
    }

    /**
     * Create a new facet from an existing sliced distinct counts map.
     * 
     * @param name the name of this facet as supplied by the user
     * @param counts the sliced distinct counts map
     */
    public InternalSlicedDistinctFacet(final String name,
            final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts) {
        super(name);
        _counts = counts;
    }

    @Override
    public long getTotalCount() {
        materialize();
        return _total;
    }

    @Override
    public long getDistinctCount() {
        materialize();
        return _distinctCount;
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
    protected ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> peekCounts() {
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

    /**
     * Prepare this facet for rendering, clearing any held data in the process.
     */
    private synchronized void materialize() {
        if(_periods != null)
            // This facet has been materialized already
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

    /**
     * Operates on the elements of a distinct counts map,
     * releasing all held data so it can be garbage collected.
     */
    private static class CacheReleaser implements TObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {

        @Override
        public boolean execute(final ExtTHashMap<BytesRef, DistinctCountPayload> map) {
            CacheRecycler.pushHashMap(map);
            return true;
        }

    }

    private final TimePeriodMerger _mergePeriods = new TimePeriodMerger();

    /**
     * Performs merge operation over the elements of a sliced distinct counts map,
     * updating the corresponding payloads in a target map.
     */
    private static final class TimePeriodMerger implements TLongObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {

        InternalSlicedDistinctFacet target;

        @Override
        public boolean execute(final long time, final ExtTHashMap<BytesRef, DistinctCountPayload> slices) {
            // Called once per period:
            // update the corresponding period in the target facet, slice by slice,
            // or add it if it's not there already

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

        /**
         * Performs merge operation over the slices in a time period,
         * updating the corresponding payloads in a target map.
         */
        private static final class SliceMerger implements TObjectObjectProcedure<BytesRef, DistinctCountPayload> {

            ExtTHashMap<BytesRef, DistinctCountPayload> target;

            @Override
            public boolean execute(final BytesRef sliceLabel, final DistinctCountPayload payload) {
                // Called once for each slice in a period
                payload.mergeInto(target, sliceLabel);
                return true;
            }

        }

    }

    private final PeriodMaterializer _materializePeriods = new PeriodMaterializer();

    /**
     * Performs materialize operation over a sliced distinct counts map, converting
     * the counts into a list of TimePeriod objects. This list will
     * be in the same order as the map entries were provided.
     */
    private static final class PeriodMaterializer implements TLongObjectProcedure<ExtTHashMap<BytesRef, DistinctCountPayload>> {

        private List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> _target;
        private DistinctCountPayload _accumulator;

        public void init(final List<DistinctTimePeriod<XContentEnabledList<DistinctSlice<String>>>> periods) {
            _target = periods;
            _accumulator = null;
        }

        @Override
        public boolean execute(final long time, final ExtTHashMap<BytesRef, DistinctCountPayload> period) {
            // Called once per time period:
            // materialize the slices in the supplied slice, then
            // update the corresponding slice in the target facet, or add if not there

            // First create output buffer for the slices from this period
            final XContentEnabledList<DistinctSlice<String>> buffer =
                    new XContentEnabledList<DistinctSlice<String>>(period.size(), Constants.SLICES);
            // Then materialize the slices into it, creating period-wise subtotals as we go along
            _materializeSlices.init(buffer);
            period.forEachEntry(_materializeSlices);
            // Save materialization results, and period-wise subtotals
            final DistinctCountPayload periodAccumulator = _materializeSlices.getAccumulator();
            final long count = periodAccumulator.getCount();
            final long cardinality = periodAccumulator.getDistinctCount();
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

        /**
         * Release any resources held by the procedure so they can
         * be garbage-collected.
         */
        public void clear() {
            _target = null;
            _accumulator = null;
            _materializeSlices.clear();
        }

        private final SliceMaterializer _materializeSlices = new SliceMaterializer();

        /**
         * Performs materialize operation over the slices in a time period,
         * updating the corresponding payloads in a target map.
         */
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
                        payload.getCount(), payload.getDistinctCount()));

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

    /**
     * Performs serialize operation over a sliced distinct counts map, writing
     * the data to an ElasticSearch StreamOutput object.
     */
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

        /**
         * Release any resources held by the procedure so they can
         * be garbage-collected.
         */
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

            /**
             * Release any resources held by the procedure so they can
             * be garbage-collected.
             */
            public void clear() {
                _output = null;
            }

        }

    }

}
