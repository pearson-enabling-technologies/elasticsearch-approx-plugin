package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.trove.set.TIntSet;
import org.elasticsearch.common.trove.set.TLongSet;
import org.elasticsearch.common.trove.set.hash.TIntHashSet;
import org.elasticsearch.common.trove.set.hash.TLongHashSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldData.StringValueProc;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.FieldDataType.DefaultTypes;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

// TODO: Auto-generated Javadoc
/**
 * The Class TermListFacetCollector.
 */
public class TermListFacetCollector extends AbstractFacetCollector {

    // FIXME make this parameterizable
    /** The _read from field cache. */
    private boolean _readFromFieldCache = false;

    /** The _facet name. */
    private final String _facetName;

    /** The _max per shard. */
    private final int _maxPerShard;

    /** The _key field name. */
    private final String _keyFieldName;

    /** The _key field type. */
    private final FieldDataType _keyFieldType;

    /** The _field data cache. */
    private final FieldDataCache _fieldDataCache;

    /** The _key field data. */
    private FieldData _keyFieldData;

    /** The _doc base. */
    private int _docBase;

    /** The _strings. */
    private Collection<String> _strings;

    /** The _ints. */
    private TIntSet _ints;

    /** The _longs. */
    private TLongSet _longs;

    /** The KeyFieldVisitor instance. */
    private final StringValueProc _proc = new KeyFieldVisitor();

    /**
     * Instantiates a new term list facet collector.
     *
     * @param facetName the facet name
     * @param keyField the key field
     * @param context the context
     * @param maxPerShard the max per shard
     * @param bReadFromCache the b read from cache
     */
    public TermListFacetCollector(final String facetName, final String keyField,
            final SearchContext context, final int maxPerShard, final boolean bReadFromCache) {
        super(facetName);
        _facetName = facetName;
        _maxPerShard = maxPerShard;
        _readFromFieldCache = bReadFromCache;

        _fieldDataCache = context.fieldDataCache();
        final MapperService.SmartNameFieldMappers keyMappers = context.smartFieldMappers(keyField);

        if(keyMappers == null || !keyMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for key field [" + keyField + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if(keyMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(keyMappers.docMapper().typeFilter()));
        }

        final FieldMapper keyMapper = keyMappers.mapper();
        _keyFieldName = keyMapper.names().indexName();
        _keyFieldType = keyMapper.fieldDataType();
        if(_keyFieldType.equals(DefaultTypes.STRING))
            _strings = newHashSet();
        else if(_keyFieldType.equals(DefaultTypes.INT))
            _ints = new TIntHashSet();
        else if(_keyFieldType.equals(DefaultTypes.LONG))
            _longs = new TLongHashSet();
    }

    // TODO make this work for other data types too

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.FacetCollector#facet()
     */
    @Override
    public Facet facet() {
        if(_strings != null)
        {
            return new InternalTermListFacet(_facetName, _strings.toArray());
        }
        else if(_ints != null)
            return new InternalTermListFacet(_facetName, _ints.toArray());
        else if(_longs != null)
            return new InternalTermListFacet(_facetName, _longs.toArray());
        else
            return new InternalTermListFacet(_facetName);
    }

    /**
     * This method gets called once for each index segment, with a new reader.
     *
     * @param reader the reader
     * @param docBase the doc base
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    protected void doSetNextReader(final IndexReader reader, final int docBase) throws IOException {
        if(_readFromFieldCache) {
            _keyFieldData = _fieldDataCache.cache(_keyFieldType, reader, _keyFieldName);
            _docBase = docBase;
        } else {

            // use this mechanism to retrieve terms from the lucene index.
            //clean the cache for this request 
            _fieldDataCache.clear("no-cache request", _keyFieldName);
            _fieldDataCache.cache(_keyFieldType, reader, _keyFieldName).forEachValue(_proc);
            /*
             * works for string fields but NOT  for int/long fields.  why? maybe switch on data type and retrieve
             * strings by looping all terms and use the mechanism above for ints/longs?
             * 
            final TermEnum terms = reader.terms();
            while(terms.next()) {
                final Term term = terms.term();
                System.out.println("the terrm is " + term.field() + " with value " + term.text());
                final ByteBuffer bbuf = ByteBuffer.allocate(100);

                String y = "";
                if(_keyFieldName.equals(term.field())) { 
                    final String value = term.text(); 
                    final byte[] bytes = value.getBytes();
                    saveValue(value);
                }
            }*/
        }
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.AbstractFacetCollector#doCollect(int)
     */
    @Override
    protected void doCollect(final int doc) throws IOException {
        if(_readFromFieldCache)
            _keyFieldData.forEachValue(_proc);
        // Otherwise do nothing -- we just read the values from the index directly
    }

    /**
     *  For each term in the visitor, save the value in the appropriate array given the field datatype
     *
     * @param value the value
     */
    private void saveValue(final String value) {
        if(_strings != null && _strings.size() <= _maxPerShard) {
            _strings.add(value);
        } else if(_ints != null && _ints.size() <= _maxPerShard) {

            try {
                _ints.add(Integer.parseInt(value));
            } catch(final Exception ex) {

            }
        }
        else if(_longs != null && _longs.size() <= _maxPerShard) {
            try {

                final long ret = Long.parseLong(value);
                _longs.add(ret);
            } catch(final Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * The Class KeyFieldVisitor.
     */
    private class KeyFieldVisitor implements StringValueProc {

        /* (non-Javadoc)
         * @see org.elasticsearch.index.field.data.FieldData.StringValueProc#onValue(java.lang.String)
         */
        @Override
        public void onValue(final String value) {
            saveValue(value);
        }
    }

}
