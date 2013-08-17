package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectIntProcedure;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacet.ComparatorType;

public class InternalSlicedFacet extends InternalFacet {

    private final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> _counts;
    private final ComparatorType _comparatorType;

    private static final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> EMPTY = new ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>>();

    public InternalSlicedFacet(final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> counts, final ComparatorType comparatorType) {
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
            final InternalSlicedFacet target = (InternalSlicedFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalSlicedFacet source = (InternalSlicedFacet) facets.get(i);
                _mergeTimePeriods.target = target;
                source._counts.forEachEntry(_mergeTimePeriods);
                _mergeTimePeriods.target = null; // Avoid risk of garbage leaks
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalSlicedFacet(EMPTY, _comparatorType);
        }
    }

    private void releaseCache() {
        _counts.forEachValue(new TObjectProcedure<TObjectIntHashMap<BytesRef>>() {
            @Override
            public boolean execute(final TObjectIntHashMap<BytesRef> subMap) {
                CacheRecycler.pushObjectIntMap(subMap);
                return true;
            }
        });
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final TimePeriodMerger _mergeTimePeriods = new TimePeriodMerger();

    private static final class TimePeriodMerger implements TLongObjectProcedure<TObjectIntHashMap<BytesRef>> {

        InternalSlicedFacet target;

        @Override
        public boolean execute(final long time, final TObjectIntHashMap<BytesRef> slices) {
            // Does this time period exist in the target facet?
            TObjectIntHashMap<BytesRef> targetPeriod = target._counts.get(time);
            // If not, then pull one from the object cache to use
            if(targetPeriod == null)
                targetPeriod = target._counts.put(time, CacheRecycler.<BytesRef> popObjectIntMap());

            // Add or update all slices
            _mergeSlices.target = targetPeriod;
            slices.forEachEntry(_mergeSlices);
            _mergeSlices.target = null; // Avoid risk of garbage leaks
            return true;
        }

        private final SliceMerger _mergeSlices = new SliceMerger();

        private static final class SliceMerger implements TObjectIntProcedure<BytesRef> {

            TObjectIntHashMap<BytesRef> target;

            @Override
            public boolean execute(final BytesRef sliceLabel, final int count) {
                // Add or update count for this slice label
                target.adjustOrPutValue(sliceLabel, count, count);
                return true;
            }

        }

    }

}
