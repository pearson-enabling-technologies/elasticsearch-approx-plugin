package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
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
    //private int _docBase;

    /** objects to collect */
    private final Collection<Object> _objects = newHashSet();

    /** The KeyFieldVisitor instance. */
    private final StringValueProc _proc = new KeyFieldVisitor();

    protected final ESLogger _logger;

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

        _logger = Loggers.getLogger(getClass());

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
    }

    // TODO make this work for other data types too

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.FacetCollector#facet()
     */
    @Override
    public Facet facet() {

        if(_objects != null) {
            return new InternalTermListFacet(_facetName, _objects.toArray());
        }
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
            if(_keyFieldData == null) {
                _keyFieldData = _fieldDataCache.cache(_keyFieldType, reader, _keyFieldName);
            }
        } else {
            // Directly retrieve terms from the lucene index
            // break the scan if any of the lists has _maxPerShard items 
            final TermEnum terms = reader.terms(new Term(_keyFieldName));
            while(true) {
                final Term term = terms.term();
                final String termText = term.text();
                final long termLen = termText.length();

                if(_keyFieldName.equals(term.field())) {
                    if(_keyFieldType.equals(DefaultTypes.STRING) && _objects.size() <= _maxPerShard) {
                        saveValue(termText);
                    }
                    else if((_objects.size() <= _maxPerShard) && termLen == NumericUtils.BUF_SIZE_INT
                            && _keyFieldType.equals(DefaultTypes.INT)) {
                        final Integer val = NumericUtils.prefixCodedToInt(termText);
                        saveValue(val);
                    }
                    else if((_objects.size() <= _maxPerShard) && termLen == NumericUtils.BUF_SIZE_LONG
                            && _keyFieldType.equals(DefaultTypes.LONG)) {
                        final Long val = NumericUtils.prefixCodedToLong(termText);
                        saveValue(val);
                    }
                    if(_objects.size() >= _maxPerShard) {
                        break;
                    }
                }
                if(!terms.next())
                    break;
            }
            terms.close();
        }

    }

    private boolean isMaxPerShardReached() {
        return _objects.size() >= _maxPerShard;

    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.AbstractFacetCollector#doCollect(int)
     */
    @Override
    protected void doCollect(final int doc) throws IOException {
        if(_readFromFieldCache) {
            if(!isMaxPerShardReached()) {
                _keyFieldData.forEachValue(_proc);
            }
        }
        // Otherwise do nothing -- we just read the values from the index directly
    }

    /**
     *  For each term in the visitor, save the value in the appropriate array given the field datatype
     *
     * @param value the value
     */
    private void saveValue(final Object value) {
        _objects.add(value);
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

            if(_objects.size() <= _maxPerShard)
                saveValue(value);
        }
    }

}
