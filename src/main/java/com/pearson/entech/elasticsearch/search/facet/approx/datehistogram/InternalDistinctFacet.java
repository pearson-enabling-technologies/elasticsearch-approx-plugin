package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;

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
        return _distinctCount; // TODO
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
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    public String getType() {
        return TYPE;
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
            final InternalDistinctFacet target = (InternalDistinctFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalDistinctFacet source = (InternalDistinctFacet) facets.get(i);
                _mergeTimePeriods.target = target;
                source._counts.forEachEntry(_mergeTimePeriods);
                _mergeTimePeriods.target = null; // Avoid risk of garbage leaks
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalDistinctFacet(getName(), EMPTY);
        }
    }

    private void releaseCache() {
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final TimePeriodMerger _mergeTimePeriods = new TimePeriodMerger();

    private static class TimePeriodMerger implements TLongObjectProcedure<DistinctCountPayload> {
        InternalDistinctFacet target;

        @Override
        public boolean execute(final long time, final DistinctCountPayload payload) {
            // These objects already know how to merge themselves
            payload.mergeInto(target._counts, time);
            return true;
        }
    }

}
