package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.iterator.TLongObjectIterator;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

/**
 *
 */
public class InternalDistinctDateHistogramFacet implements DistinctDateHistogramFacet, InternalFacet {

    private static final String STREAM_TYPE = "distinct_date_histogram";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final String type, final StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     */
    public static class CountEntry implements Entry {
        private final long time;
        private final long count;
        private final long distinctCount;

        public CountEntry(final long time, final long count, final long distinctCount) {
            this.time = time;
            this.count = count;
            this.distinctCount = distinctCount;
        }

        @Override
        public long time() {
            return time;
        }

        @Override
        public long getTime() {
            return time();
        }

        @Override
        public long count() {
            return count;
        }

        @Override
        public long getCount() {
            return count();
        }

        @Override
        public long distinctCount() {
            return distinctCount;
        }

        @Override
        public long getDistinctCount() {
            return distinctCount();
        }
    }

    private String name;

    private ComparatorType comparatorType;

    ExtTLongObjectHashMap<DistinctCountPayload> counts;
    boolean cachedCounts;

    CountEntry[] entries = null;

    private InternalDistinctDateHistogramFacet() {
    }

    public InternalDistinctDateHistogramFacet(final String name, final ComparatorType comparatorType,
            final ExtTLongObjectHashMap<DistinctCountPayload> counts, final boolean cachedCounts) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.counts = counts;
        this.cachedCounts = cachedCounts;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return type();
    }

    @Override
    public List<CountEntry> entries() {
        return Arrays.asList(computeEntries());
    }

    @Override
    public List<CountEntry> getEntries() {
        return entries();
    }

    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) entries().iterator();
    }

    void releaseCache() {
        if(cachedCounts) {
            CacheRecycler.pushLongObjectMap(counts);
            cachedCounts = false;
            counts = null;
        }
    }

    // TODO we are unnecessarily serializing and deserializing in the reduce phase

    private CountEntry[] computeEntries() {
        if(entries != null) {
            return entries;
        }
        entries = new CountEntry[counts.size()];
        int i = 0;
        for(final TLongObjectIterator<DistinctCountPayload> it = counts.iterator(); it.hasNext();) {
            it.advance();
            final DistinctCountPayload payload = it.value();
            entries[i++] = new CountEntry(it.key(), payload.getCount(), payload.getCardinality().cardinality());
        }
        releaseCache();
        Arrays.sort(entries, comparatorType.comparator());
        return entries;
    }

    public Facet reduce(final String name, final List<Facet> facets) {
        if(facets.size() == 1) {
            return facets.get(0);
        }
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();

        for(final Facet facet : facets) {
            final InternalDistinctDateHistogramFacet histoFacet = (InternalDistinctDateHistogramFacet) facet;
            for(final TLongObjectIterator<DistinctCountPayload> it = histoFacet.counts.iterator(); it.hasNext();) {
                it.advance();
                final long facetStart = it.key();
                if(counts.containsKey(facetStart))
                    try {
                        counts.put(facetStart, counts.get(facetStart).merge(it.value()));
                    } catch(final CardinalityMergeException e) {
                        throw new ElasticSearchException("Unable to merge two facet cardinality objects", e);
                    }
                else
                    counts.put(facetStart, counts.get(facetStart));
            }
            histoFacet.releaseCache();
        }

        return new InternalDistinctDateHistogramFacet(name, comparatorType, counts, true);
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for(final Entry entry : computeEntries()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.time());
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalDistinctDateHistogramFacet readHistogramFacet(final StreamInput in) throws IOException {
        final InternalDistinctDateHistogramFacet facet = new InternalDistinctDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());

        final int size = in.readVInt();
        counts = CacheRecycler.popLongObjectMap();
        cachedCounts = true;
        for(int i = 0; i < size; i++) {
            final long key = in.readLong();
            final long count = in.readVLong();
            final int cardLength = in.readVInt();
            final byte[] cardinality = new byte[cardLength];
            in.read(cardinality, 0, cardLength);
            try {
                counts.put(key, new DistinctCountPayload(count, cardinality));
            } catch(final ClassNotFoundException e) {
                throw new ElasticSearchException("Unable to deserialize facet cardinality object", e);
            }
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(counts.size());
        for(final TLongObjectIterator<DistinctCountPayload> it = counts.iterator(); it.hasNext();) {
            it.advance();
            out.writeLong(it.key());
            out.writeVLong(it.value().getCount());
            final byte[] cardinality = it.value().cardinalityBytes();
            out.writeVInt(cardinality.length);
            out.write(cardinality);
        }
        releaseCache();
    }
}