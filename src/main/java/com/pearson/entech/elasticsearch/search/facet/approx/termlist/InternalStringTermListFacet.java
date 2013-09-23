package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.deserialize;
import static com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.merge;
import static com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.process;
import static com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.serialize;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;

import com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.AsStrings;

public class InternalStringTermListFacet extends InternalTermListFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("tTermList"));

    private     Constants.FIELD_DATA_TYPE _dataType = Constants.FIELD_DATA_TYPE.UNDEFINED;

    private static final BytesRefHash EMPTY = new BytesRefHash();
    static {
        EMPTY.close();
    }

    private BytesRefHash _bytesRefs;

    private List<String> _strings;

    InternalStringTermListFacet() {
        _bytesRefs = new BytesRefHash();
    }

    InternalStringTermListFacet(final String facetName) {
        super(facetName);
        _bytesRefs = new BytesRefHash();
    }

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    InternalStringTermListFacet(final String facetName, final BytesRefHash terms, final Constants.FIELD_DATA_TYPE dataType) {
        super(facetName);
        _bytesRefs = terms;
        _dataType = dataType;

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
        materialize();
        return _strings;
    }

    @Override
    public Iterator<String> iterator() {
        materialize();
        return _strings.iterator();
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
        builder.field(Fields.ENTRIES, getEntries());
        builder.endObject();
        return builder;
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            final int count = facets.size();
            final BytesRefHash[] hashes = new BytesRefHash[count];
            for(int i = 0; i < count; i++) {
                hashes[i] = ((InternalStringTermListFacet) facets.get(i))._bytesRefs;
            }
            merge(hashes);
            return facets.get(0);
        } else {
            return new InternalStringTermListFacet(getName(), EMPTY, Constants.FIELD_DATA_TYPE.UNDEFINED);
        }
    }

    public static InternalStringTermListFacet readTermListFacet(final StreamInput in) throws IOException {
        final InternalStringTermListFacet facet = new InternalStringTermListFacet();

        facet.readFrom(in);
        return facet;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        serialize(_bytesRefs, out);
        _bytesRefs = null;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        _bytesRefs = deserialize(in);
    }

    private synchronized void materialize() {
        if(_strings != null)
            return;

        final AsStrings proc = new AsStrings(_bytesRefs.size(), _dataType);
        process(_bytesRefs, proc);
        _strings = proc.getList();
        _bytesRefs = null;
    }

}
