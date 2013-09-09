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
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.BytesValues.Iter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

public class TermListFacetExecutor extends FacetExecutor {

    private final Random _random = new Random(0);

    private final int _maxPerShard;
    private final String _facetName;
    private final float _sampleRate;
    private final boolean _exhaustive;

    private final IndexFieldData<?> _indexFieldData;

    BytesRefHash _entries = new BytesRefHash();

    public TermListFacetExecutor(final SearchContext context, final IndexFieldData<?> indexFieldData,
            final String facetName, final int maxPerShard, final float sample) {
        _maxPerShard = maxPerShard;
        _sampleRate = sample;
        _exhaustive = _sampleRate > 0.995;
        _facetName = facetName;
        _indexFieldData = indexFieldData;
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        return new InternalStringTermListFacet(facetName, _entries);
    }

    @Override
    public Collector collector() {
        return new CollectorExecutor();
    }

    @Override
    public Post post() {
        // TODO would we need to implement filtering separately at this stage? From SearchContext?
        //        return new PostExecutor(_indexFieldData.getFieldNames().name());
        throw new UnsupportedOperationException("Post aggregation is not yet supported");
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
        public void postCollection() {}

    }

    // First attempt -- not yet working -- doesn't decode numeric fields properly
    final class PostExecutor extends FacetExecutor.Post {

        private final String _fieldName;

        public PostExecutor(final String fieldName) {
            _fieldName = fieldName;
        }

        @Override
        public void executePost(final List<ContextDocIdSet> docSets) throws IOException {
            TermsEnum termsEnum = null;
            DocsEnum docsEnum = null;
            for(final ContextDocIdSet docSet : docSets) {
                final AtomicReader reader = docSet.context.reader();
                // TODO Do we need to filter out deleted docs? reader.getLiveDocs()
                final Bits visibleDocs = docSet.docSet.bits();

                final Terms terms = reader.terms(_fieldName);
                termsEnum = terms.iterator(termsEnum);
                BytesRef ref;
                while((ref = termsEnum.next()) != null) {
                    docsEnum = termsEnum.docs(visibleDocs, docsEnum);
                    if(docsEnum.nextDoc() != DocsEnum.NO_MORE_DOCS) {
                        // We have a hit in at least one doc
                        _entries.add(ref);
                        //                        System.out.println("Added value " + ref);
                        if(_entries.size() == _maxPerShard)
                            return;
                    }
                }
            }
        }

    }

}
