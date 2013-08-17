package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.procedure.TLongIntProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

public class InternalCountingFacet extends InternalFacet implements TimeFacet<TimePeriod<Long>> {

    private final String _name;
    private final TLongIntHashMap _counts;

    private long _total;
    private List<TimePeriod<Long>> _periods;

    private static final TLongIntHashMap EMPTY = new TLongIntHashMap();
    private static final String TYPE = "CountingDateHistogramFacet";
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    public InternalCountingFacet(final String name, final TLongIntHashMap counts) {
        super(name);
        _name = name;
        _counts = counts;
    }

    private static class Period extends TimePeriod<Long> {

        private final long _time;
        private final long _count;

        Period(final long time, final long count) {
            _time = time;
            _count = count;
        }

        @Override
        public long getTime() {
            return _time;
        }

        @Override
        public Long getEntry() {
            return _count;
        }

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
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(_name);
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
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            // Reduce into the first facet; we will release its _counts on materializing
            final InternalCountingFacet target = (InternalCountingFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalCountingFacet source = (InternalCountingFacet) facets.get(i);
                // For each datetime period in the new facet...
                _mergeTimePeriods.target = target;
                source._counts.forEachEntry(_mergeTimePeriods);
                _mergeTimePeriods.target = null;
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalCountingFacet(_name, EMPTY);
        }
    }

    private synchronized void materialize() {
        final Period[] periods = new Period[_counts.size()];
        _materializePeriods.init(periods);
        _counts.forEachEntry(_materializePeriods);
        Arrays.sort(periods);
        releaseCache();
    }

    private void releaseCache() {
        CacheRecycler.pushLongIntMap(_counts);
    }

    private final TimePeriodMerger _mergeTimePeriods = new TimePeriodMerger();

    private static final class TimePeriodMerger implements TLongIntProcedure {

        InternalCountingFacet target;

        @Override
        public boolean execute(final long time, final int count) {
            // Increment the corresponding count in the target facet, or add if not there
            target._counts.adjustOrPutValue(time, count, count);
            return true;
        }

    }

    private final PeriodMaterializer _materializePeriods = new PeriodMaterializer();

    private static final class PeriodMaterializer implements TLongIntProcedure {

        private Period[] _target;
        private int _pointer;

        public void init(final Period[] target) {
            _target = target;
            _pointer = 0;
        }

        @Override
        public boolean execute(final long time, final int count) {
            _target[_pointer] = new Period(time, count);
            _pointer++;
            return true;
        }

    }

}
