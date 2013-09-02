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

public abstract class BytesFieldFirstCollector<B extends AtomicFieldData<? extends ScriptDocValues>, V extends AtomicFieldData<? extends ScriptDocValues>>
        extends BuildableCollector {

    private final IndexFieldData<B> _bytesFieldData;
    private final IndexFieldData<V> _valueFieldData;
    private BytesValues _bytesFieldValues;
    private BytesValues _valueFieldValues;
    private IntsRef _docOrds;
    private int _docOrdPointer;
    private Iter _docIter;
    private Iter _valueFieldIter;

    public BytesFieldFirstCollector(
            final IndexFieldData<B> bytesFieldData,
            final IndexFieldData<V> valueFieldData) {
        _bytesFieldData = bytesFieldData;
        _valueFieldData = valueFieldData;
    }

    public BytesFieldFirstCollector(
            final IndexFieldData<B> bytesFieldData) {
        _bytesFieldData = bytesFieldData;
        _valueFieldData = null;
    }

    @Override
    public void collect(final int doc) throws IOException {
        if(_bytesFieldValues instanceof WithOrdinals) {
            _docOrds = ((WithOrdinals) _bytesFieldValues).ordinals().getOrds(doc);
            _docOrdPointer = _docOrds.offset;
        } else {
            _docIter = _bytesFieldValues.getIter(doc);
        }
        if(hasValueField())
            _valueFieldIter = _valueFieldValues.getIter(doc);
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        _bytesFieldValues = _bytesFieldData.load(context).getBytesValues();
        if(hasValueField())
            _valueFieldValues = _valueFieldData.load(context).getBytesValues();
    }

    protected boolean hasNextBytes() {
        if(_bytesFieldValues instanceof WithOrdinals) {
            return _docOrdPointer < _docOrds.length;
        } else {
            return _docIter.hasNext();
        }
    }

    protected BytesRef nextBytes() {
        if(_bytesFieldValues instanceof WithOrdinals) {
            final BytesRef safe = ((WithOrdinals) _bytesFieldValues).getSafeValueByOrd(_docOrdPointer);
            _docOrdPointer++;
            return safe;
        } else {
            return _docIter.next();
        }
    }

    protected boolean hasValueField() {
        return _valueFieldData != null;
    }

    protected boolean hasNextValue() {
        return _valueFieldIter != null && _valueFieldIter.hasNext();
    }

    protected BytesRef nextValue() {
        return _valueFieldIter == null ? null : _valueFieldIter.next();
    }

}
