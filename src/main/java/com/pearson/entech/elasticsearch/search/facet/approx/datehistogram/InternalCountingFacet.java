package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.procedure.TLongIntProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacet.ComparatorType;

public class InternalCountingFacet extends InternalFacet {

    private final TLongIntHashMap _counts;
    private final ComparatorType _comparatorType;

    private static final TLongIntHashMap EMPTY = new TLongIntHashMap();

    public InternalCountingFacet(final TLongIntHashMap counts, final ComparatorType comparatorType) {
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
        return null;
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
            return new InternalCountingFacet(EMPTY, _comparatorType);
        }
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

}
