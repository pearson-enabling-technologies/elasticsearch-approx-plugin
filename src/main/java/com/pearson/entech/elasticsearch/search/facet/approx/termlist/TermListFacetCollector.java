package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Sets.newHashSet;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.elasticsearch.common.trove.set.TIntSet;
import org.elasticsearch.common.trove.set.hash.TIntHashSet;
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

public class TermListFacetCollector extends AbstractFacetCollector {

    // FIXME make this parameterizable
    private final boolean _readFromFieldCache = false;

    private final String _facetName;
    private final int _maxPerShard;

    private final String _keyFieldName;
    private final FieldDataType _keyFieldType;
    private final FieldDataCache _fieldDataCache;
    private FieldData _keyFieldData;
    private int _docBase;

    private Collection<String> _strings;

    private TIntSet _ints;

    private final StringValueProc _proc = new KeyFieldVisitor();

    public TermListFacetCollector(final String facetName, final String keyField, final SearchContext context, final int maxPerShard) {
        super(facetName);
        _facetName = facetName;
        _maxPerShard = maxPerShard;

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

    }

    // TODO make this work for other data types too

    @Override
    public Facet facet() {
        if(_strings != null)
            return new InternalTermListFacet(_facetName, (String[]) _strings.toArray());
        else if(_ints != null)
            return new InternalTermListFacet(_facetName, _ints.toArray());
        else
            return new InternalTermListFacet(_facetName);
    }

    /**
     * This method gets called once for each index segment, with a new reader. 
     */
    @Override
    protected void doSetNextReader(final IndexReader reader, final int docBase) throws IOException {
        if(_readFromFieldCache) {
            _keyFieldData = _fieldDataCache.cache(_keyFieldType, reader, _keyFieldName);
            _docBase = docBase;
        } else {
            final TermEnum terms = reader.terms();
            while(terms.next()) {
                final Term term = terms.term();
                if(_keyFieldName.equals(term.field())) {
                    final String value = term.text();
                    saveValue(value);
                }
            }
        }
    }

    @Override
    protected void doCollect(final int doc) throws IOException {
        if(_readFromFieldCache)
            _keyFieldData.forEachValue(_proc);
        // Otherwise do nothing -- we just read the values from the index directly
    }

    private void saveValue(final String value) {
        if(_strings != null && _strings.size() <= _maxPerShard) {
            _strings.add(value);
        } else if(_ints != null && _ints.size() <= _maxPerShard) {
            _ints.add(Integer.parseInt(value));
        }
    }

    private class KeyFieldVisitor implements StringValueProc {

        @Override
        public void onValue(final String value) {
            saveValue(value);
        }
    }

}
