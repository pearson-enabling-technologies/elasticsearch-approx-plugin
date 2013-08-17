package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

public class InternalDistinctFacet extends TimeFacet<DistinctTimePeriod<Long>> implements HasDistinct {

    private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;

    private long _total;
    private List<DistinctTimePeriod<Long>> _periods;
    private long _distinctCount;

    private static final ExtTLongObjectHashMap<DistinctCountPayload> EMPTY = new ExtTLongObjectHashMap<DistinctCountPayload>();
    private static final String TYPE = "DistinctDateHistogramFacet";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    public InternalDistinctFacet(final String name, final ExtTLongObjectHashMap<DistinctCountPayload> counts) {
        super(name);
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
    public List<DistinctTimePeriod<Long>> getTimePeriods() {
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
        builder.startObject(getName());
        builder.field(Constants._TYPE, TYPE);
        builder.field(Constants.COUNT, getTotal());
        builder.field(Constants.DISTINCT_COUNT, getDistinctCount());
        builder.startArray(Constants.ENTRIES);
        for(final DistinctTimePeriod<Long> period : _periods) {
            builder.field(Constants.TIME, period.getTime());
            builder.field(Constants.COUNT, period.getEntry());
            builder.field(Constants.DISTINCT_COUNT, period.getDistinctCount());
        }
        builder.endArray();
        builder.endObject();
        return builder;
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

    // TODO better checking for 0-length collections

    private synchronized void materialize() {
        _periods = newArrayListWithCapacity(_counts.size());
        _materializePeriod.init(_periods);
        _counts.forEachEntry(_materializePeriod);
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = _materializePeriod.getOverallTotal();
        _distinctCount = _materializePeriod.getOverallDistinct();
        releaseCache();
    }

    private void releaseCache() {
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

        private List<DistinctTimePeriod<Long>> _target;
        private DistinctCountPayload _accumulator;

        public void init(final List<DistinctTimePeriod<Long>> target) {
            _target = target;
            _accumulator = null;
        }

        public long getOverallTotal() {
            return _accumulator.getCount();
        }

        public long getOverallDistinct() {
            return _accumulator.getCardinality().cardinality();
        }

        // Called once per period
        @Override
        public boolean execute(final long time, final DistinctCountPayload payload) {
            final long count = payload.getCount();
            final long cardinality = payload.getCardinality().cardinality();
            _target.add(new DistinctTimePeriod<Long>(time, count, cardinality));

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

    }

}
