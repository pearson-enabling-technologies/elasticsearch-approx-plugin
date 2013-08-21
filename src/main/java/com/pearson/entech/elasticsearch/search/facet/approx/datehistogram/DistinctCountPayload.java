package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

public class DistinctCountPayload {

    private long _count;

    private CountThenEstimateBytes _cardinality;

    public DistinctCountPayload(final int entryLimit) {
        _count = 0;
        //        _cardinality = new CountThenEstimateBytes(entryLimit,
        //                new HyperLogLog.Builder(0.0025));
        //        _cardinality = new CountThenEstimateBytes(entryLimit, AdaptiveCounting.Builder.obyCount(1000000000));
        //        _cardinality = new CountThenEstimateBytes(entryLimit, new HyperLogLog.Builder(0.0005));
        _cardinality = new CountThenEstimateBytes(entryLimit, new HyperLogLogPlus.Builder(14, 25));
    }

    DistinctCountPayload(final long count, final CountThenEstimateBytes cardinality) {
        _count = count;
        _cardinality = cardinality;
    }

    DistinctCountPayload(final long count, final byte[] cardinality) throws IOException, ClassNotFoundException {
        _count = count;
        _cardinality = new CountThenEstimateBytes(cardinality);
    }

    DistinctCountPayload update(final BytesRef unsafe) {
        _count++;
        _cardinality.offerBytesRef(unsafe);
        return this;
    }

    byte[] cardinalityBytes() throws IOException {
        return _cardinality.getBytes();
    }

    long getCount() {
        return _count;
    }

    CountThenEstimateBytes getCardinality() {
        return _cardinality;
    }

    DistinctCountPayload merge(final DistinctCountPayload other) throws CardinalityMergeException {
        _count += other._count;
        _cardinality = CountThenEstimateBytes.mergeEstimators(this._cardinality, other._cardinality);
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

    <K> DistinctCountPayload mergeInto(final ExtTHashMap<K, DistinctCountPayload> map, final K key) {
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

    @Override
    public String toString() {
        final String descr = _cardinality.sizeof() == -1 ?
                "Set" : "Estimator";
        return String.format(
                "%s of %d distinct elements (%d total elements)",
                descr, _cardinality.cardinality(), _count);
    }

}