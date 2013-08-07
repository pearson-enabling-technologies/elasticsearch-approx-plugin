package com.pearson.entech.elasticsearch.facet.approx.datehistogram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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

    public LongInternalDistinctDateHistogramFacet(final String name, final ComparatorType comparatorType,
            final ExtTLongObjectHashMap<DistinctCountPayload> counts, final boolean cachedCounts) {
        super(name);
        this.comparatorType = comparatorType;
        this.tEntries = counts;
        this.cachedEntries = cachedCounts;
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
        return new LongInternalDistinctDateHistogramFacet(getName());
    }

    /**
     * The reader for the internal transport protocol.
     */
    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        comparatorType = ComparatorType.fromId(in.readByte());

        cachedEntries = false;
        final int size = in.readVInt();
        entries = new ArrayList<DistinctEntry>(size);
        for(int i = 0; i < size; i++) {
            final long time = in.readLong();
            final int nameSize = in.readVInt();
            final Set<Object> names = new HashSet<Object>(nameSize);
            for(int j = 0; j < nameSize; j++) {
                names.add(in.readLong());
            }
            entries.add(new DistinctEntry(time, names));
        }
    }

    /**
     * The writer for the internal transport protocol.
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(comparatorType.id());
        out.writeVInt(entries.size());
        for(final DistinctEntry entry : entries) {
            out.writeLong(entry.getTime());
            out.writeVInt(entry.getValues().size());
            for(final Object name : entry.getValues()) {
                out.writeLong((Long) name);
            }
        }
        releaseCache();
    }
}
