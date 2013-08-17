package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;


public class InternalDistinctFacet extends InternalFacet {

    private final ExtTLongObjectHashMap<DistinctCountPayload> _counts;
    private final ComparatorType _comparatorType;

    private static final ExtTLongObjectHashMap<DistinctCountPayload> EMPTY = new ExtTLongObjectHashMap<DistinctCountPayload>();

    public InternalDistinctFacet(final ExtTLongObjectHashMap<DistinctCountPayload> counts, final ComparatorType comparatorType) {
        _counts = counts;
        _comparatorType = comparatorType;
    }

    @Override
    public String getType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // TODO Auto-generated method stub
        releaseCache();
    }

    @Override
    public BytesReference streamType() {
        // TODO Auto-generated method stub
        return null;
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
            return new InternalDistinctFacet(EMPTY, _comparatorType);
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
