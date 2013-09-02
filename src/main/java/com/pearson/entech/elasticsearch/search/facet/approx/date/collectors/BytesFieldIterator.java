package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.BytesValues.Iter;
import org.elasticsearch.index.fielddata.BytesValues.WithOrdinals;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;

public class BytesFieldIterator<B extends AtomicFieldData<? extends ScriptDocValues>>
        extends CollectableIterator<BytesRef> {

    private final IndexFieldData<B> _bytesFieldData;
    private BytesValues _bytesFieldValues;
    private IntsRef _docOrds;
    private int _docOrdPointer;
    private Iter _docIter;

    public BytesFieldIterator(
            final IndexFieldData<B> bytesFieldData) {
        _bytesFieldData = bytesFieldData;
    }

    @Override
    public void collect(final int doc) throws IOException {
        if(_bytesFieldValues instanceof WithOrdinals) {
            _docOrds = ((WithOrdinals) _bytesFieldValues).ordinals().getOrds(doc);
            _docOrdPointer = _docOrds.offset;
        } else {
            _docIter = _bytesFieldValues.getIter(doc);
        }
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        _bytesFieldValues = _bytesFieldData.load(context).getBytesValues();
    }

    @Override
    public boolean hasNext() {
        if(_bytesFieldValues instanceof WithOrdinals) {
            return _docOrdPointer < _docOrds.length;
        } else {
            return _docIter.hasNext();
        }
    }

    @Override
    public BytesRef next() {
        if(_bytesFieldValues instanceof WithOrdinals) {
            final BytesRef unsafe = ((WithOrdinals) _bytesFieldValues).getSafeValueByOrd(_docOrds.ints[_docOrdPointer]);
            _docOrdPointer++;
            return unsafe;
        } else {
            return _docIter.next();
        }
    }

    @Override
    public void postCollection() {
        //        _docOrds = null;
        //        _docOrdPointer = -1;
        //        _docIter = null;
    }

}
