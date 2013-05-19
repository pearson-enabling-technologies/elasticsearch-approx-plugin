/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.iterator.TLongObjectIterator;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;

/**
 *
 */
public class InternalDistinctCountDateHistogramFacet extends InternalDateHistogramFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray("cdHistogram");

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
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
        public long getTime() {
            return time;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public long getTotalCount() {
            return 0;
        }

        @Override
        public double getTotal() {
            return Double.NaN;
        }

        @Override
        public double getMean() {
            return Double.NaN;
        }

        @Override
        public double getMin() {
            return Double.NaN;
        }

        @Override
        public double getMax() {
            return Double.NaN;
        }

        @Override
        public long time() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long count() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long distinctCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public long getDistinctCount() {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    private ComparatorType comparatorType;

    ExtTLongObjectHashMap<DistinctCountPayload> counts;
    boolean cachedCounts;
    CountEntry[] entries = null;

    private Text name;

    public InternalDistinctCountDateHistogramFacet(final String name, final ComparatorType comparatorType,
            final ExtTLongObjectHashMap<DistinctCountPayload> counts,
            final boolean cachedCounts) {
        super(name);
        this.comparatorType = comparatorType;
        this.counts = counts;
        this.cachedCounts = cachedCounts;
    }

    private InternalDistinctCountDateHistogramFacet() {
        // Just for deserialization
    }

    @Override
    public List<CountEntry> getEntries() {
        return Arrays.asList(computeEntries());
    }

    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) getEntries().iterator();
    }

    void releaseCache() {
        if(cachedCounts) {
            CacheRecycler.pushLongObjectMap(counts);
            cachedCounts = false;
            counts = null;
        }
    }

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

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() == 1) {
            return facets.get(0);
        }
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();

        for(final Facet facet : facets) {
            final InternalDistinctCountDateHistogramFacet histoFacet = (InternalDistinctCountDateHistogramFacet) facet;
            for(final TLongObjectIterator<DistinctCountPayload> it = histoFacet.counts.iterator(); it.hasNext();) {
                it.advance();
                it.advance();
                final long facetStart = it.key();
                it.value().mergeInto(counts, facetStart);
            }
            histoFacet.releaseCache();

        }

        return new InternalDistinctCountDateHistogramFacet(getName(), comparatorType, counts, true);
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for(final Entry entry : computeEntries()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.getTime());
            builder.field(Fields.COUNT, entry.getCount());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalDistinctCountDateHistogramFacet readHistogramFacet(final StreamInput in) throws IOException {
        final InternalDistinctCountDateHistogramFacet facet = new InternalDistinctCountDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        name = in.readText();
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
        out.writeText(name);
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

    @Override
    public List<? extends Entry> entries() {
        // TODO Auto-generated method stub
        return null;
    }
}