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
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectLongHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectLongProcedure;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.search.facet.Facet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.external.Constants;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.Slice;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.TimePeriod;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.XContentEnabledList;

public class InternalSlicedFacet extends DateFacet<TimePeriod<XContentEnabledList<Slice<String>>>> {

    private ExtTLongObjectHashMap<TObjectLongHashMap<BytesRef>> _counts;

    private long _total;
    private List<TimePeriod<XContentEnabledList<Slice<String>>>> _periods;

    private static final ExtTLongObjectHashMap<TObjectLongHashMap<BytesRef>> EMPTY = new ExtTLongObjectHashMap<TObjectLongHashMap<BytesRef>>();
    static final String TYPE = "sliced_date_facet";
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

    public static InternalSlicedFacet readHistogramFacet(final StreamInput in) throws IOException {
        final InternalSlicedFacet facet = new InternalSlicedFacet();
        facet.readFrom(in);
        return facet;
    }

    // Only for deserialization
    protected InternalSlicedFacet() {
        super("not set");
    }

    public InternalSlicedFacet(final String facetName, final ExtTLongObjectHashMap<TObjectLongHashMap<BytesRef>> counts) {
        super(facetName);
        _counts = counts;
    }

    @Override
    public long getTotalCount() {
        materialize();
        return _total;
    }

    @Override
    public List<TimePeriod<XContentEnabledList<Slice<String>>>> getTimePeriods() {
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
    protected ExtTLongObjectHashMap<TObjectLongHashMap<BytesRef>> peekCounts() {
        return _counts;
    }

    @Override
    protected void readData(final StreamInput in) throws IOException {
        _counts = CacheRecycler.popLongObjectMap();
        final int size = in.readVInt();
        for(int i = 0; i < size; i++) {
            final long key = in.readVLong();
            final long sliceCount = in.readVLong();
            final TObjectLongHashMap<BytesRef> slice = new TObjectLongHashMap<BytesRef>(); // no CacheRecycler for these
            for(int j = 0; j < sliceCount; j++) {
                final BytesRef sliceLabel = in.readBytesRef();
                slice.put(sliceLabel, in.readVInt());
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
        if(_periods != null)
            return;
        if(_counts == null || _counts.size() == 0) {
            _total = 0;
            _periods = newArrayListWithCapacity(0);
            return;
        }
        _periods = newArrayListWithCapacity(_counts.size());
        final long[] counter = { 0 };
        _materializePeriods.init(_periods, counter);
        _counts.forEachEntry(_materializePeriods);
        _materializePeriods.clear();
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = counter[0];
        releaseCache();
    }

    @Override
    protected void releaseCache() {
        _counts.forEachValue(new TObjectProcedure<TObjectLongHashMap<BytesRef>>() {
            @Override
            public boolean execute(final TObjectLongHashMap<BytesRef> subMap) {
                subMap.clear(); // no CacheRecycler for these
                return true;
            }
        });
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final PeriodMerger _mergePeriods = new PeriodMerger();

    private static final class PeriodMerger implements TLongObjectProcedure<TObjectLongHashMap<BytesRef>> {

        InternalSlicedFacet target;

        // Called once per period
        @Override
        public boolean execute(final long time, final TObjectLongHashMap<BytesRef> slices) {
            // Does this time period exist in the target facet?
            TObjectLongHashMap<BytesRef> targetPeriod = target._counts.get(time);
            // If not, then pull one from the object cache to use
            if(targetPeriod == null) {
                targetPeriod = new TObjectLongHashMap<BytesRef>(); // no CacheRecycler for these
                target._counts.put(time, targetPeriod);
            }

            // Add or update all slices in this period
            _mergeSlices.target = targetPeriod;
            slices.forEachEntry(_mergeSlices);
            _mergeSlices.target = null; // Reduce risk of garbage leaks
            return true;
        }

        private final SliceMerger _mergeSlices = new SliceMerger();

        private static final class SliceMerger implements TObjectLongProcedure<BytesRef> {

            TObjectLongHashMap<BytesRef> target;

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef sliceLabel, final long count) {
                // Add or update count for this slice label
                target.adjustOrPutValue(sliceLabel, count, count);
                return true;
            }

        }

    }

    private final PeriodMaterializer _materializePeriods = new PeriodMaterializer();

    private static final class PeriodMaterializer implements TLongObjectProcedure<TObjectLongHashMap<BytesRef>> {

        private List<TimePeriod<XContentEnabledList<Slice<String>>>> _target;
        private long[] _totalCounter;
        private long[] _periodCounter;

        public void init(final List<TimePeriod<XContentEnabledList<Slice<String>>>> periods, final long[] totalCounter) {
            _target = periods;
            _totalCounter = totalCounter;
            _periodCounter = new long[1];
        }

        // Called once per time period
        @Override
        public boolean execute(final long time, final TObjectLongHashMap<BytesRef> period) {
            // First create _output buffer for the slices from this period
            final XContentEnabledList<Slice<String>> buffer =
                    new XContentEnabledList<Slice<String>>(period.size(), Constants.SLICES);
            // Reset period counter
            _periodCounter[0] = 0;
            // Then materialize the slices into it
            _materializeSlices.init(buffer, _periodCounter);
            period.forEachEntry(_materializeSlices);
            // Add period counter to total counter
            _totalCounter[0] += _periodCounter[0];
            // Finally save results
            _target.add(
                    new TimePeriod<XContentEnabledList<Slice<String>>>(
                            time, _periodCounter[0], buffer));
            return true;
        }

        public void clear() {
            _target = null;
            _materializeSlices.clear();
        }

        private final SliceMaterializer _materializeSlices = new SliceMaterializer();

        private static final class SliceMaterializer implements TObjectLongProcedure<BytesRef> {

            private List<Slice<String>> _target;
            private long[] _counter;

            public void init(final List<Slice<String>> target, final long[] counter) {
                _target = target;
                _counter = counter;
            }

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef key, final long count) {
                _target.add(new Slice<String>(key.utf8ToString(), count));
                _counter[0] += count;
                return true;
            }

            public void clear() {
                _target = null;
            }

        }

    }

    private final PeriodSerializer _serializePeriods = new PeriodSerializer();

    private static final class PeriodSerializer implements TLongObjectProcedure<TObjectLongHashMap<BytesRef>> {

        private StreamOutput _output;

        public void init(final StreamOutput output, final int size) throws IOException {
            _output = output;
            output.writeVInt(size);
        }

        // Called once per time period
        @Override
        public boolean execute(final long key, final TObjectLongHashMap<BytesRef> period) {
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

        private static final class SliceSerializer implements TObjectLongProcedure<BytesRef> {

            private StreamOutput _output;

            public void init(final StreamOutput output, final int size) throws IOException {
                _output = output;
                output.writeVInt(size);
            }

            // Called once for each slice in a period
            @Override
            public boolean execute(final BytesRef sliceLabel, final long count) {
                try {
                    _output.writeBytesRef(sliceLabel);
                    _output.writeVLong(count);
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
