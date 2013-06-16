package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

// TODO: Auto-generated Javadoc
/**
 * The Class InternalTermListFacet.
 */
public class InternalTermListFacet implements TermListFacet, InternalFacet {

    ESLogger _logger = Loggers.getLogger(getClass());

    /** The Constant STREAM_TYPE. */
    private static final String STREAM_TYPE = "term_list";

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

    /** The _objects. */
    private Object[] _objects; // dataType 0

    /** The _name. */
    private String _name; // plugin name

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
        _objects = strings;

    }

    /**
     * Instantiates a new internal integer term list facet.
     *
     * @param facetName is the facet name
     * @param ints, the integer array
     */

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
        _objects = new Object[0];
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
    @SuppressWarnings("deprecation")
    @Override
    public void readFrom(final StreamInput in) throws IOException {

        try {
            _name = in.readUTF();
            final int size = in.readVInt();
            //final byte dataType = in.readByte();
            _objects = CacheRecycler.popObjectArray(size);
            for(int i = 0; i < size; i++) {
                _objects[i] = in.readUTF();
            }
        } catch(final Exception ex) {
            _logger.error("[readFrom: Exception ( " + _name + ")]", ex);
        }
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.io.stream.Streamable#writeTo(org.elasticsearch.common.io.stream.StreamOutput)
     */
    @SuppressWarnings("deprecation")
    @Override
    public void writeTo(final StreamOutput out) throws IOException {

        final String[] strArray = new String[_objects.length];
        for(int i = 0; i < _objects.length; i++) {
            strArray[i] = _objects[i].toString();
        }
        out.writeUTF(_name);
        out.writeVInt(_objects.length);
        for(int i = 0; i < strArray.length; i++)
            out.writeUTF(strArray[i]);
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
        builder.array(Fields.ENTRIES, _objects);
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
        if(_objects != null)
            CacheRecycler.pushObjectArray(_objects);

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
        return Arrays.asList(_objects);
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
        final Collection<Object> list = newHashSet();
        for(final Facet facet : facets) {
            final InternalTermListFacet itlf = (InternalTermListFacet) facet;
            for(final Object obj : itlf._objects) {
                if(obj != null)
                    list.add(obj.toString());
            }
        } 
        final Object[] ret = list.toArray(new Object[list.size()]);
        return new InternalTermListFacet(name, ret);

    }
}
