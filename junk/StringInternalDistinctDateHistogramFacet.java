package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;


/*
 *
 */
public class StringInternalDistinctDateHistogramFacet extends InternalDistinctDateHistogramFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray("DistinctDateHistogram".getBytes());

    public static void registerStreams() {
        InternalFacet.Streams.registerStream(STREAM, STREAM_TYPE);
    }

    StringInternalDistinctDateHistogramFacet(final String name) {
        super(name);
    }

    public StringInternalDistinctDateHistogramFacet(final String name,
            final ComparatorType comparatorType,
            final ExtTLongObjectHashMap<DistinctCountPayload> counts,
            final boolean cachedCounts) {
        super(name);
        this.comparatorType = comparatorType;
        this.counts = counts;
        this.cachedCounts = cachedCounts;
    }

    @Override
    protected InternalDistinctDateHistogramFacet newFacet() {
        return new StringInternalDistinctDateHistogramFacet(getName());
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    public StringInternalDistinctDateHistogramFacet() {
        super();
    }

    public static StringInternalDistinctDateHistogramFacet readHistogramFacet(final StreamInput in) throws IOException {
        final StringInternalDistinctDateHistogramFacet facet = new StringInternalDistinctDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

}