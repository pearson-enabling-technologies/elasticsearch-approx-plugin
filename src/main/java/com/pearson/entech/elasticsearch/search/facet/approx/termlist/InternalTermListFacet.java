package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.google.common.primitives.Ints;

public class InternalTermListFacet implements TermListFacet, InternalFacet {

    private static final String STREAM_TYPE = "term_list";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final String type, final StreamInput in) throws IOException {
            return readTermListFacet(in);
        }
    };

    public static InternalTermListFacet readTermListFacet(final StreamInput in) throws IOException {
        final InternalTermListFacet facet = new InternalTermListFacet();
        facet.readFrom(in);
        return facet;
    }

    private byte _dataType = -1; // -1 == uninitialized

    private Object[] _strings; // dataType 0

    private int[] _ints; // dataType 1

    private String _name;

    private String _type;

    public InternalTermListFacet(final String facetName, final String[] strings) {
        _name = facetName;
        _strings = strings;
        _dataType = 0;
    }

    public InternalTermListFacet(final String facetName, final int[] ints) {
        _name = facetName;
        _ints = ints;
        _dataType = 1;
    }

    private InternalTermListFacet() {}

    public InternalTermListFacet(final String facetName) {
        _name = facetName;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public String getType() {
        return _type;
    }

    @Override
    public String name() {
        return _name;
    }

    @Override
    public String type() {
        return _type;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        _name = in.readString();
        final int size = in.readVInt();
        final byte dataType = in.readByte();
        switch(dataType) {
        case -1:
            _strings = null;
            _ints = null;
            break;
        case 0:
            _strings = CacheRecycler.popObjectArray(size);
            _ints = null;
            break;
        case 1:
            _strings = null;
            _ints = CacheRecycler.popIntArray(size);
            break;
        default:
            throw new IllegalArgumentException("dataType " + dataType + " is not known");
        }
        for(int i = 0; i < size; i++) {
            switch(dataType) {
            case 0:
                _strings[i] = in.readString();
                break;
            case 1:
                _ints[i] = in.readInt();
                break;
            }
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(_name);
        out.writeByte(_dataType);
        switch(_dataType) {
        case -1:
            out.writeVInt(0);
            out.writeByte((byte) -1);
            break;
        case 0:
            out.writeVInt(_strings.length);
            out.writeStringArray((String[]) _strings);
            break;
        case 1:
            for(final int i : _ints) {
                out.writeVInt(i);
            }
            break;
        }
        releaseCache();
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(_name);
        builder.field(Fields._TYPE, STREAM_TYPE);
        switch(_dataType) {
        case -1:
            break;
        case 0:
            builder.array(Fields.ENTRIES, _strings);
            break;
        case 1:
            builder.array(Fields.ENTRIES, _ints);
            break;
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    private void releaseCache() {
        if(_strings != null)
            CacheRecycler.pushObjectArray(_strings);
        if(_ints != null)
            CacheRecycler.pushIntArray(_ints);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Object> iterator() {
        return (Iterator<Object>) entries().iterator();
    }

    @Override
    public List<? extends Object> entries() {
        switch(_dataType) {
        case 0:
            return Arrays.asList(_strings);
        case 1:
            return Ints.asList(_ints);
        }
        return newArrayList();
    }

    @Override
    public List<? extends Object> getEntries() {
        return entries();
    }

    public Facet reduce(final List<Facet> facets) {
        final Set<? extends Object> merged = new HashSet<Object>();
        for(final Facet facet : facets) {
            final InternalTermListFacet itlf = (InternalTermListFacet) facet;
            switch(_dataType) {
            case 0:
                // TODO
                break;
            case 1:
                // TODO
                break;
            default:
                // TODO throw exception
            }
        }
        // TODO Auto-generated method stub
        return null;
    }

}
