package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.facet.Facet;


public class LongInternalDistinctDateHistogramFacet extends InternalDistinctDateHistogramFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray("LongDistinctDateHistogram".getBytes());

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    LongInternalDistinctDateHistogramFacet() {}

    LongInternalDistinctDateHistogramFacet(final String name) {
        super(name);
    }

    public LongInternalDistinctDateHistogramFacet(final String name,
            final ComparatorType comparatorType,
            final ExtTLongObjectHashMap<DistinctCountPayload> counts,
            final boolean cachedCounts) {
        super(name);
        this.comparatorType = comparatorType;
        this.counts = counts;
        this.cachedCounts = cachedCounts;
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    public static LongInternalDistinctDateHistogramFacet readHistogramFacet(final StreamInput in) throws IOException {
        final LongInternalDistinctDateHistogramFacet facet = new LongInternalDistinctDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    protected LongInternalDistinctDateHistogramFacet newFacet() {
        return new LongInternalDistinctDateHistogramFacet(name);
    }

}
