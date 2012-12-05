package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.security.InvalidParameterException;
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

// TODO: Auto-generated Javadoc
/**
 * The Class InternalTermListFacet.
 */
public class InternalTermListFacet implements TermListFacet, InternalFacet {

    /** The Constant STREAM_TYPE. */
    private static final String STREAM_TYPE = "term_list_facet";

    /**
     * Register streams.
     */
    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    /** The stream. */
    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final String type, final StreamInput in) throws IOException {
            return readTermListFacet(in);
        }
    };

    /**
     * Read term list facet.
     *
     * @param in the in
     * @return the internal term list facet
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static InternalTermListFacet readTermListFacet(final StreamInput in) throws IOException {
        final InternalTermListFacet facet = new InternalTermListFacet();
        facet.readFrom(in);
        return facet;
    }

    /** The _data type. */
    private byte _dataType = -1; // -1 == uninitialized

    /** The _strings. */
    private Object[] _strings; // dataType 0

    /** The _ints. */
    private int[] _ints; // dataType 1

    /** The _longs. */
    private Long[] _longs;  //dataType 2 

    /** The _name. */
    private String _name;   // plugin name

    /** The _type. */
    private final String _type = STREAM_TYPE;

    /**
     * Instantiates a new internal string term list facet.
     *
     * @param facetName the facet name
     * @param strings the strings
     */
    public InternalTermListFacet(final String facetName, final Object[] strings) {
        _name = facetName;
        _strings = strings;
        _dataType = 0;
    }

    
    /**
     * Instantiates a new internal integer term list facet.
     *
     * @param facetName is the facet name
     * @param ints, the integer array
     */
    public InternalTermListFacet(final String facetName, final int[] ints) {
        _name = facetName;
        _ints = ints;
        _dataType = 1;
    }

    /**
     * Instantiates a new internal long term list facet.
     *
     * @param facetName the facet name
     * @param longs array of longs 
     */
    public InternalTermListFacet(final String facetName, final long[] longs) {
        _name = facetName;
        _longs = new Long[longs.length];
        for(int l = 0; l < longs.length; l++)
            _longs[l] = longs[l];

        _dataType = 2;
    }

    /**
     * Instantiates a new internal term list facet.
     */
    private InternalTermListFacet() {
    }

    /**
     * Instantiates a new internal term list facet.
     *
     * @param facetName the facet name
     */
    public InternalTermListFacet(final String facetName) {
        _name = facetName;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.Facet#getName()
     */
    @Override
    public String getName() {
        return _name;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.Facet#getType()
     */
    @Override
    public String getType() {
        return _type;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.Facet#name()
     */
    @Override
    public String name() {
        return _name;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.Facet#type()
     */
    @Override
    public String type() {
        return _type;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.io.stream.Streamable#readFrom(org.elasticsearch.common.io.stream.StreamInput)
     */
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
        case 2:
            _strings = null;
            _ints = null;
            
            //allocate object array, no popLongArray in the CacheRecycler object
            _longs = (Long[]) CacheRecycler.popObjectArray(size);

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
            case 2:
                _longs[i] = in.readLong();
                break;
            }
        }
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.io.stream.Streamable#writeTo(org.elasticsearch.common.io.stream.StreamOutput)
     */
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
        case 2: 
            for(final Long i : _longs) {
                out.writeVLong(i);
            }
        }
        releaseCache();
    }

    /**
     * Output JSON fields
     */
    static final class Fields {
        
        /** The Constant _TYPE. */
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        
        /** The Constant ENTRIES. */
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.xcontent.ToXContent#toXContent(org.elasticsearch.common.xcontent.XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)
     */
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
        case 2:
            builder.array(Fields.ENTRIES,  (Object[])_longs);
            break;
        }
        builder.endObject();
        return builder;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.InternalFacet#streamType()
     */
    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    /**
     * Release cache.
     */
    private void releaseCache() {
        if(_strings != null)
            CacheRecycler.pushObjectArray(_strings);
        if(_ints != null)
            CacheRecycler.pushIntArray(_ints);
        if(_longs != null)
            CacheRecycler.pushObjectArray(_longs);

    }

    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Object> iterator() {
        return entries().iterator();
    }

    /* (non-Javadoc)
     * @see com.pearson.entech.elasticsearch.search.facet.approx.termlist.TermListFacet#entries()
     */
    @Override
    public List<Object> entries() {

        switch(_dataType) {
        case 0:
            return Arrays.asList(_strings);
        case 1:
            final List<Object> ret = newArrayList();
            for(final int i : _ints)
                ret.add(i);
            return ret;
        case 2:
            final List<Object> retL = newArrayList();
            for(final long l : _longs)
                retL.add(l);
            return retL;
        }
        return newArrayList();

    }

    /* (non-Javadoc)
     * @see com.pearson.entech.elasticsearch.search.facet.approx.termlist.TermListFacet#getEntries()
     */
    @Override
    public List<? extends Object> getEntries() {
        return entries();
    }

    /**
     * Takes a list of facets and returns a new facet containing the merged data from all of them.
     *
     * @param name the facet name
     * @param facets the facets
     * @return the resulting reduced facet
     */
    public Facet reduce(final String name, final List<Facet> facets) {

        System.out.println("[" + name + " reducing]");

        final Set<String> reducedStrings = new HashSet<String>();
        final Set<Integer> reducedInts = new HashSet<Integer>();
        final Set<Long> reducedLongs = new HashSet<Long>();

        for(final Facet facet : facets) {
            final InternalTermListFacet itlf = (InternalTermListFacet) facet;
            switch(_dataType) {
            case 0:

                for(final Object obj : itlf._strings) {
                    reducedStrings.add(obj.toString());
                } 
                break;
            case 1:

                for(final Object obj : itlf._ints) {
                    reducedInts.add(Integer.parseInt(obj.toString()));
                }
                break;
            case 2:

                for(final Long obj : itlf._longs) {
                    reducedLongs.add(Long.parseLong(obj.toString()));
                } 
                break;
            default:
                throw new InvalidParameterException("Data type not supported for this plugin");
            }
        }

        switch(_dataType) {
        case 0:
            final Object[] strings = reducedStrings.toArray(new Object[reducedStrings.size()]);
            return new InternalTermListFacet(name, strings);
        case 1:
            final Object[] ints = reducedInts.toArray(new Object[reducedInts.size()]);
            return new InternalTermListFacet(name, ints);
        case 2:
            final Object[] longs = reducedLongs.toArray(new Object[reducedLongs.size()]);
            return new InternalTermListFacet(name, longs);
        default:
            return null;
        }

    }

}
