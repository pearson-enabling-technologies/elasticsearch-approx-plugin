package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.set.hash.THashSet;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;

import com.google.common.collect.ImmutableList;

public class InternalStringTermListFacet extends InternalTermListFacet {
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("tTermList"));

    private InternalStringTermListFacet() {

    }

    InternalStringTermListFacet(final String facetName) {
        super(facetName);
    }

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    Collection<String> entries = ImmutableList.of();

    InternalStringTermListFacet(final String facetName, final THashSet<String> hashSet) {
        super(facetName);
    }

    InternalStringTermListFacet(final String facetName, final List<String> items) {
        super(facetName);
        entries = items;
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            return readTermListFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @Override
    public List<? extends String> getEntries() {

        if(!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<String>) entries;

    }

    @Override
    public Iterator<String> iterator() {
        return entries.iterator();
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        /** The Constant ENTRIES. */
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");

    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject(getName());
        builder.field(Fields._TYPE, STREAM_TYPE);
        //builder.array(Fields.ENTRIES, entries); //not array here, causes [ [ "x", "y", "z"] ] output
        builder.field(Fields.ENTRIES, entries);
        builder.endObject();
        return builder;

    }

    @Override
    public Facet reduce(final List<Facet> facets) {

        final Set<String> items = new HashSet<String>();

        if(facets.size() == 1) {
            return facets.get(0);
        }
        final THashSet<String> aggregated = CacheRecycler.popHashSet();
        for(final Facet facet : facets) {
            final InternalTermListFacet termListFacet = (InternalTermListFacet) facet;

            for(final String entry : termListFacet.getEntries()) {
                aggregated.add(entry);
                items.add(entry);

            }
        }
        CacheRecycler.pushHashSet(aggregated);

        final List<String> ret = new ArrayList<String>();
        ret.addAll(items);

        return new InternalStringTermListFacet(facets.get(0).getName(), ret);

    }

    public static InternalStringTermListFacet readTermListFacet(final StreamInput in) throws IOException {
        final InternalStringTermListFacet facet = new InternalStringTermListFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {

        super.writeTo(out);
        out.writeVInt(entries.size());
        for(final String entry : entries) {
            out.writeString(entry);
        }
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        final int size = in.readVInt();
        final List<String> entries = new ArrayList<String>(size);
        for(int i = 0; i < size; i++) {
            entries.add(in.readString());
        }

    }

}
