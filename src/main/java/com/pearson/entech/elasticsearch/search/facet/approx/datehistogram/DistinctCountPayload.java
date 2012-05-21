package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.trove.map.TLongObjectMap;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.CountThenEstimate;

public class DistinctCountPayload {

    private long _count;

    private CountThenEstimate _cardinality;

    public DistinctCountPayload() {
        _count = 0;
        _cardinality = new CountThenEstimate();
    }

    DistinctCountPayload(final long count, final CountThenEstimate cardinality) {
        _count = count;
        _cardinality = cardinality;
    }

    DistinctCountPayload(final long count, final byte[] cardinality) throws IOException, ClassNotFoundException {
        _count = count;
        _cardinality = new CountThenEstimate(cardinality);
    }

    DistinctCountPayload update(final Object item) {
        _count++;
        _cardinality.offer(item);
        return this;
    }

    byte[] cardinalityBytes() throws IOException {
        return _cardinality.getBytes();
    }

    long getCount() {
        return _count;
    }

    CountThenEstimate getCardinality() {
        return _cardinality;
    }

    DistinctCountPayload merge(final DistinctCountPayload other) throws CardinalityMergeException {
        _count += other._count;
        _cardinality = CountThenEstimate.mergeEstimators(this._cardinality, other._cardinality);
        return this;
    }

    DistinctCountPayload mergeInto(final TLongObjectMap<DistinctCountPayload> map, final long key) {
        if(map.containsKey(key))
            try {
                map.put(key, this.merge(map.get(key)));
            } catch(final CardinalityMergeException e) {
                throw new ElasticSearchException("Unable to merge two facet cardinality objects", e);
            }
        else
            map.put(key, this);
        return this;
    }
}
