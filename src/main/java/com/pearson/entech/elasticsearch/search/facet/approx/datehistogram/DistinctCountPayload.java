package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.CountThenEstimate;

public class DistinctCountPayload {

    private long _count;

    private CountThenEstimate _cardinality;

    DistinctCountPayload(final long count, final CountThenEstimate cardinality) {
        _count = count;
        _cardinality = cardinality;
    }

    DistinctCountPayload(final long count, final byte[] cardinality) throws IOException, ClassNotFoundException {
        _count = count;
        _cardinality = new CountThenEstimate(cardinality);
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
}
