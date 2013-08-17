package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.procedure.TLongIntProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;

public class InternalCountingFacet extends TimeFacet<TimePeriod<Long>> {

    private final TLongIntHashMap _counts;

    private long _total;
    private List<TimePeriod<Long>> _periods;

    private static final TLongIntHashMap EMPTY = new TLongIntHashMap();
    private static final String TYPE = "CountingDateHistogramFacet";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    public InternalCountingFacet(final String name, final TLongIntHashMap counts) {
        super(name);
        _counts = counts;
    }

    @Override
    public long getTotal() {
        materialize();
        return _total;
    }

    @Override
    public List<TimePeriod<Long>> getTimePeriods() {
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
        builder.startArray(Constants.ENTRIES);
        for(final TimePeriod<Long> period : _periods) {
            builder.field(Constants.TIME, period.getTime());
            builder.field(Constants.COUNT, period.getEntry());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            // Reduce into the first facet; we will release its _counts on materializing
            final InternalCountingFacet target = (InternalCountingFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalCountingFacet source = (InternalCountingFacet) facets.get(i);
                // For each datetime period in the new facet...
                _mergePeriods.target = target;
                source._counts.forEachEntry(_mergePeriods);
                _mergePeriods.target = null;
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalCountingFacet(getName(), EMPTY);
        }
    }

    private synchronized void materialize() {
        _periods = newArrayListWithCapacity(_counts.size());
        final long[] counter = { 0 };
        _materializePeriod.init(_periods, counter);
        _counts.forEachEntry(_materializePeriod);
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = counter[0];
        releaseCache();
    }

    private void releaseCache() {
        CacheRecycler.pushLongIntMap(_counts);
    }

    private final PeriodMerger _mergePeriods = new PeriodMerger();

    private static final class PeriodMerger implements TLongIntProcedure {

        InternalCountingFacet target;

        // Called once per period
        @Override
        public boolean execute(final long time, final int count) {
            // Increment the corresponding count in the target facet, or add if not there
            target._counts.adjustOrPutValue(time, count, count);
            return true;
        }

    }

    private final PeriodMaterializer _materializePeriod = new PeriodMaterializer();

    private static final class PeriodMaterializer implements TLongIntProcedure {

        private List<TimePeriod<Long>> _target;
        private long[] _counter;

        public void init(final List<TimePeriod<Long>> periods, final long[] counter) {
            _target = periods;
            _counter = counter;
        }

        // Called once per period
        @Override
        public boolean execute(final long time, final int count) {
            _target.add(new TimePeriod<Long>(time, (long) count));
            _counter[0] = _counter[0] + count;
            return true;
        }

    }

}
