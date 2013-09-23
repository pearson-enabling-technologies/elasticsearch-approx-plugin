package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.BytesValues.Iter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import com.pearson.entech.elasticsearch.search.facet.approx.termlist.Constants.FIELD_DATA_TYPE;

public class TermListFacetExecutor extends FacetExecutor {

    private final Random _random = new Random(0);

    private final int _maxPerShard;
    private final String _facetName;
    private final float _sampleRate;
    private final boolean _exhaustive;
    private Constants.FIELD_DATA_TYPE _type;

    private final IndexFieldData<?> _indexFieldData;

    BytesRefHash _entries = new BytesRefHash();

    public TermListFacetExecutor(final SearchContext context, final IndexFieldData<?> indexFieldData,
            final String facetName, final int maxPerShard, final float sample) {
        _maxPerShard = maxPerShard;
        _sampleRate = sample;
        _exhaustive = _sampleRate > 0.995;
        _facetName = facetName;
        _indexFieldData = indexFieldData;
        _type = getType();

    }

    
    /**
     * gets the field data type from the _indexFieldData object 
     * @return
     */
    private Constants.FIELD_DATA_TYPE getType() {
        final boolean numericField = (_indexFieldData instanceof IndexNumericFieldData);
        FIELD_DATA_TYPE type = FIELD_DATA_TYPE.STRING;
        if(numericField) {
            final IndexNumericFieldData<?> indexNumericFieldData = (IndexNumericFieldData<?>) _indexFieldData;
            if(indexNumericFieldData.getNumericType() == IndexNumericFieldData.NumericType.LONG)
                type = FIELD_DATA_TYPE.LONG;
            if(indexNumericFieldData.getNumericType() == IndexNumericFieldData.NumericType.INT)
                type = FIELD_DATA_TYPE.INT;
        }
        return type;
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {

        _type = getType();
        return new InternalStringTermListFacet(facetName, _entries, _type);
    }

    @Override
    public Collector collector() {
        return new CollectorExecutor();
    }

    @Override
    public Post post() {
        // TODO would we need to implement filtering separately at this stage? From SearchContext?

        //check the data type of the field
        //if it is numeric, get the type and pass the information to the PostExecutor
        //otherwise, handle strings

        final boolean numericField = (_indexFieldData instanceof IndexNumericFieldData);
        FIELD_DATA_TYPE type = FIELD_DATA_TYPE.STRING;
        if(numericField) {
            final IndexNumericFieldData<?> indexNumericFieldData = (IndexNumericFieldData<?>) _indexFieldData;
            if(indexNumericFieldData.getNumericType() == IndexNumericFieldData.NumericType.LONG)
                type = FIELD_DATA_TYPE.LONG;
            if(indexNumericFieldData.getNumericType() == IndexNumericFieldData.NumericType.INT)
                type = FIELD_DATA_TYPE.INT;
        }
        return new PostExecutor(_indexFieldData.getFieldNames().name(), numericField, type);
        //throw new UnsupportedOperationException("Post aggregation is not yet supported");
    }

    final class CollectorExecutor extends FacetExecutor.Collector {

        private BytesValues _values;

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            final int currentCount = _entries.size();
            if(currentCount > _maxPerShard)
                return;

            // Heuristic: only load the values with hashes if we're in exhaustive
            // mode and we aren't coming close to hitting our per-shard limit.
            // If either of these conditions are false, load the data without
            // hashes, and they'll get calculated on the fly anyway.

            _values = (_exhaustive && currentCount * 1.1 < _maxPerShard) ?
                    _indexFieldData.load(context).getHashedBytesValues() :
                    _indexFieldData.load(context).getBytesValues();
        }

        @Override
        public void collect(final int docId) throws IOException {
            if(_entries.size() > _maxPerShard)
                return;
            if(!_exhaustive && _random.nextFloat() > _sampleRate)
                return;

            final Iter iter = _values.getIter(docId);
            while(iter.hasNext() && _entries.size() < _maxPerShard) {
                _entries.add(iter.next(), iter.hash());
            }
        }

        @Override
        public void postCollection() {
        }

    }

    // First attempt -- not yet working -- doesn't decode numeric fields properly
    final class PostExecutor extends FacetExecutor.Post {

        private final String _fieldName;
        private final boolean _numericField;
        FIELD_DATA_TYPE _type;

        public PostExecutor(final String fieldName, final boolean numericField, final FIELD_DATA_TYPE type) {
            _fieldName = fieldName;
            _numericField = numericField;
            _type = type;
        }

        @Override
        public void executePost(final List<ContextDocIdSet> docSets) throws IOException {
            TermsEnum termsEnum = null;
            DocsEnum docsEnum = null;
            for(final ContextDocIdSet docSet : docSets) {
                final AtomicReader reader = docSet.context.reader();
                // TODO Ensure this filters out deleted docs correctly
                final Bits visibleDocs = docSet.docSet.bits();

                final Terms terms = reader.terms(_fieldName);
                termsEnum = terms.iterator(termsEnum);
                BytesRef ref;
                while((ref = termsEnum.next()) != null) {
                    docsEnum = termsEnum.docs(visibleDocs, docsEnum);
                    if(docsEnum.nextDoc() != DocsEnum.NO_MORE_DOCS) {
                        // We have a hit in at least one doc

                        //check if we have a numeric field and then check ref.len to match with the type
                        //otherwise it is string, treat as usual
                        if(_numericField) {
                            if(_type == FIELD_DATA_TYPE.LONG && ref.length == NumericUtils.BUF_SIZE_LONG)
                            {
                                _entries.add(ref);
                            }
                            else if(_type == FIELD_DATA_TYPE.INT && ref.length == NumericUtils.BUF_SIZE_INT)
                            {
                                _entries.add(ref);
                            }
                        }
                        else {
                            _entries.add(ref);
                        }

                        if(_entries.size() == _maxPerShard)
                            return;
                    }
                }
            }
        }

    }

}
