package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.BytesValues.Iter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

public class TermListFacetExecutor extends FacetExecutor {

    private final int _maxPerShard;
    private final String _facetName;
    private final IndexFieldData<?> _indexFieldData;

    BytesRefHash _entries = new BytesRefHash();

    public TermListFacetExecutor(final IndexFieldData<?> indexFieldData, final String facetName, final int maxPerShard) {
        _maxPerShard = maxPerShard;
        _facetName = facetName;
        _indexFieldData = indexFieldData;
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        return new InternalStringTermListFacet(facetName, _entries);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    final class Collector extends FacetExecutor.Collector {

        private BytesValues _values;

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            if(_entries.size() > _maxPerShard)
                return;

            _values = _indexFieldData.load(context).getHashedBytesValues();
        }

        @Override
        public void collect(final int docId) throws IOException {
            if(_entries.size() > _maxPerShard)
                return;

            final Iter iter = _values.getIter(docId);
            while(iter.hasNext() && _entries.size() < _maxPerShard) {
                _entries.add(iter.next(), iter.hash());
            }
        }

        @Override
        public void postCollection() {}

    }

}
