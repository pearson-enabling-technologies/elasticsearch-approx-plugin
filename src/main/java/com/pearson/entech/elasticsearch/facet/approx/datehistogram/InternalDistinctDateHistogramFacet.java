package com.pearson.entech.elasticsearch.facet.approx.datehistogram;

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
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;

/**
 */
public abstract class InternalDistinctDateHistogramFacet extends InternalFacet implements DistinctDateHistogramFacet {

    public static final String TYPE = "distinct_date_histogram";
    protected ComparatorType comparatorType;

    ExtTLongObjectHashMap<DistinctCountPayload> counts;
    DistinctCountPayload overallCount;
    boolean cachedCounts;
    protected String name;

    public InternalDistinctDateHistogramFacet() {}

    public InternalDistinctDateHistogramFacet(final String facetName) {
        super(facetName);
        this.name = facetName;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static void registerStreams() {
        LongInternalDistinctDateHistogramFacet.registerStreams();
        StringInternalDistinctDateHistogramFacet.registerStreams();
    }

    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     *
     * It holds a set of distinct values and the time.
     */
    public static class InternalEntry implements Entry {
        private final long time;
        private final long distinctCount;
        private final long totalCount;

        public InternalEntry(final long time, final long distinctCount, final long totalCount) {
            this.time = time;
            this.distinctCount = distinctCount;
            this.totalCount = totalCount;
        }

        @Override
        public long getTime() {
            return time;
        }

        @Override
        public long getDistinctCount() {
            return this.distinctCount;
        }

        @Override
        public long getTotalCount() {
            return this.totalCount;
        }
    }

    public List<Entry> entries() {

        // Materialize entries
        final Entry[] entries = new InternalEntry[counts.size()];
        counts.forEachEntry(new TLongObjectProcedure<DistinctCountPayload>() {
            int idx = 0;

            @Override
            public boolean execute(final long histoKey, final DistinctCountPayload payload) {
                entries[idx] = new InternalEntry(histoKey,
                        payload.getCardinality().cardinality(), payload.getCount());
                idx++;
                return true;
            }
        });

        // Sort and return them
        Arrays.sort(entries, comparatorType.comparator());
        return Arrays.asList(entries);
    }

    @Override
    public List<Entry> getEntries() {
        return entries();
    }

    @Override
    public Iterator<Entry> iterator() {
        return entries().iterator();
    }

    void releaseCache() {
        if(cachedCounts) {
            CacheRecycler.pushLongObjectMap(counts);
            cachedCounts = false;
            counts = null;
        }
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString DISTINCT_COUNT = new XContentBuilderString("distinct_count");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString TOTAL_DISTINCT_COUNT = new XContentBuilderString("total_distinct_count");
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();

        System.out.println("Merging " + facets.size() + " facets");
        for(final Facet facet : facets) {
            final InternalDistinctDateHistogramFacet histoFacet = (InternalDistinctDateHistogramFacet) facet;
            System.out.println("Merging facet " + histoFacet);
            for(final TLongObjectIterator<DistinctCountPayload> it = histoFacet.counts.iterator(); it.hasNext();) {
                it.advance();
                final long facetKey = it.key();
                it.value().mergeInto(counts, facetKey);
            }
            histoFacet.releaseCache();
        }

        final InternalDistinctDateHistogramFacet ret = newFacet();
        ret.comparatorType = comparatorType;
        ret.counts = counts;
        ret.cachedCounts = true;
        System.out.println("Returning merged facet " + ret);
        return ret;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(this.getClass().getSimpleName()).append(": ");
        counts.forEachEntry(new TLongObjectProcedure<DistinctCountPayload>() {
            @Override
            public boolean execute(final long histoKey, final DistinctCountPayload count) {
                sb.append(String.format("%d={%s}, ", histoKey, count));
                return true;
            }
        });
        return counts.size() > 0 ? sb.substring(0, sb.lastIndexOf(",")) : sb.toString();
    }

    protected abstract InternalDistinctDateHistogramFacet newFacet();

    /**
     * Builds the final JSON result.
     *
     * For each time entry we provide the number of distinct values in the time range.
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(this.name);
        builder.field(Fields._TYPE, TYPE);
        builder.field(Fields.TOTAL_COUNT, getTotalCount());
        builder.field(Fields.TOTAL_DISTINCT_COUNT, getDistinctCount());
        builder.startArray(Fields.ENTRIES);
        for(final Entry entry : entries()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.getTime());
            builder.field(Fields.COUNT, entry.getTotalCount());
            builder.field(Fields.DISTINCT_COUNT, entry.getDistinctCount());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public long getDistinctCount() {
        return overallCount().getCardinality().cardinality();
    }

    @Override
    public long getTotalCount() {
        return overallCount().getCount();
    }

    private synchronized DistinctCountPayload overallCount() {
        if(overallCount == null) {
            overallCount = new DistinctCountPayload(Integer.MAX_VALUE);
            counts.forEachValue(new TObjectProcedure<DistinctCountPayload>() {
                @Override
                public boolean execute(final DistinctCountPayload slice) {
                    try {
                        overallCount.merge(slice);
                    } catch(final CardinalityMergeException e) {
                        throw new IllegalStateException(e);
                    }
                    return true;
                }
            });
        }
        return overallCount;
    }

    /**
     * The reader for the internal transport protocol.
     */
    @Override
    public void readFrom(final StreamInput in) throws IOException {
        name = in.readString();
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

    /**
     * The writer for the internal transport protocol.
     */
    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(getName());
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