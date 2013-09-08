package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.search.facet.Facet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DistinctDateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DistinctTimePeriod;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.HasDistinct;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.NullEntry;

public class InternalDistinctFacet extends DistinctDateFacet<DistinctTimePeriod<NullEntry>> implements HasDistinct {

    private ExtTLongObjectHashMap<DistinctCountPayload> _counts;

    private long _total;
    private List<DistinctTimePeriod<NullEntry>> _periods;
    private long _distinctCount;

    private final boolean _debug;

    private static final ExtTLongObjectHashMap<DistinctCountPayload> EMPTY = new ExtTLongObjectHashMap<DistinctCountPayload>();
    static final String TYPE = "distinct_date_facet";
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

    public static InternalDistinctFacet readHistogramFacet(final StreamInput in) throws IOException {
        final InternalDistinctFacet facet = new InternalDistinctFacet();
        facet.readFrom(in);
        return facet;
    }

    // Only for deserialization
    protected InternalDistinctFacet() {
        super("not set");
        _debug = false;
    }

    public InternalDistinctFacet(final String name, final ExtTLongObjectHashMap<DistinctCountPayload> counts) {
        super(name);
        _counts = counts;
        _debug = false;
    }

    public InternalDistinctFacet(final String name, final ExtTLongObjectHashMap<DistinctCountPayload> counts, final boolean debug) {
        super(name);
        _counts = counts;
        _debug = debug;
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
    public List<DistinctTimePeriod<NullEntry>> getTimePeriods() {
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
    protected ExtTLongObjectHashMap<DistinctCountPayload> peekCounts() {
        return _counts;
    }

    @Override
    protected void readData(final StreamInput in) throws IOException {
        _counts = CacheRecycler.popLongObjectMap();
        final int size = in.readVInt();
        for(int i = 0; i < size; i++) {
            final long key = in.readVLong();
            _counts.put(key, new DistinctCountPayload(in));
        }
    }

    @Override
    protected void writeData(final StreamOutput out) throws IOException {
        if(_counts == null) {
            out.writeVInt(0);
            return;
        }
        final int size = _counts.size();
        _serialize.init(out, size);
        _counts.forEachEntry(_serialize);
        _serialize.clear();
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            // Reduce into the first facet; we will release its _counts on rendering into XContent
            final InternalDistinctFacet target = (InternalDistinctFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalDistinctFacet source = (InternalDistinctFacet) facets.get(i);
                _mergePeriods.target = target;
                source._counts.forEachEntry(_mergePeriods);
                _mergePeriods.target = null; // Avoid risk of garbage leaks
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalDistinctFacet(getName(), EMPTY);
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
        _materializePeriod.init(_periods);
        _counts.forEachEntry(_materializePeriod);
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = _materializePeriod.getOverallTotal();
        _distinctCount = _materializePeriod.getOverallDistinct();
        _materializePeriod.clear();
        releaseCache();
    }

    @Override
    protected void releaseCache() {
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final PeriodMerger _mergePeriods = new PeriodMerger();

    private static class PeriodMerger implements TLongObjectProcedure<DistinctCountPayload> {

        InternalDistinctFacet target;

        // Called once per period
        @Override
        public boolean execute(final long time, final DistinctCountPayload payload) {
            // These objects already know how to merge themselves
            payload.mergeInto(target._counts, time);
            return true;
        }

    }

    private final PeriodMaterializer _materializePeriod = new PeriodMaterializer();

    private static final class PeriodMaterializer implements TLongObjectProcedure<DistinctCountPayload> {

        private List<DistinctTimePeriod<NullEntry>> _target;
        private DistinctCountPayload _accumulator;

        public void init(final List<DistinctTimePeriod<NullEntry>> target) {
            _target = target;
            _accumulator = null;
        }

        public long getOverallTotal() {
            return _accumulator == null ?
                    0 : _accumulator.getCount();
        }

        public long getOverallDistinct() {
            return _accumulator == null ?
                    0 : _accumulator.getCardinality().cardinality();
        }

        // Called once per period
        @Override
        public boolean execute(final long time, final DistinctCountPayload payload) {
            final long count = payload.getCount();
            final long cardinality = payload.getCardinality().cardinality();
            _target.add(new DistinctTimePeriod<NullEntry>(
                    time, count, cardinality, NullEntry.INSTANCE));

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

    private final Serializer _serialize = new Serializer();

    private static final class Serializer implements TLongObjectProcedure<DistinctCountPayload> {

        private StreamOutput _output;

        public void init(final StreamOutput output, final int size) throws IOException {
            _output = output;
            output.writeVInt(size);
        }

        // Called once per period
        @Override
        public boolean execute(final long key, final DistinctCountPayload payload) {
            try {
                _output.writeVLong(key);
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
