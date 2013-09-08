package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.common.trove.list.array.TLongArrayList;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.LongValues.Iter;
import org.elasticsearch.index.fielddata.LongValues.WithOrdinals;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;

public abstract class TimestampFirstCollector<V extends AtomicFieldData<? extends ScriptDocValues>> extends BuildableCollector {

    protected static final Iter EMPTY = new Iter.Empty();

    private LongValues _keyFieldValues;
    private IntsRef _docOrds;
    private int _docOrdPointer;
    private final TLongArrayList _timestamps = new TLongArrayList();
    private final TIntArrayList _ordToTimestampPointers = new TIntArrayList();
    private Iter _docIter;
    private long _lastNonOrdDatetime = 0;
    private long _lastNonOrdTimestamp = 0;
    private final TimeZoneRounding _tzRounding;
    private final LongArrayIndexFieldData _keyFieldData;
    private final IndexFieldData<V> _valueFieldData;

    private BytesValues _valueFieldValues;
    private BytesValues.Iter _valueFieldIter;

    public TimestampFirstCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<V> valueFieldData, final TimeZoneRounding tzRounding) {
        _keyFieldData = keyFieldData;
        _valueFieldData = valueFieldData;
        _tzRounding = tzRounding;
    }

    public TimestampFirstCollector(final LongArrayIndexFieldData keyFieldData,
            final TimeZoneRounding tzRounding) {
        this(keyFieldData, null, tzRounding);
    }

    @Override
    public void collect(final int doc) throws IOException {
        if(_keyFieldValues instanceof WithOrdinals) {
            _docOrds = ((WithOrdinals) _keyFieldValues).ordinals().getOrds(doc);
            _docOrdPointer = _docOrds.offset;
        } else {
            _docIter = _keyFieldValues.getIter(doc);
        }
        if(hasValueField())
            _valueFieldIter = _valueFieldValues.getIter(doc);
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        _keyFieldValues = _keyFieldData.load(context).getLongValues();
        if(hasValueField())
            _valueFieldValues = _valueFieldData.load(context).getBytesValues();

        if(_keyFieldValues instanceof WithOrdinals) {
            final int maxOrd = ((WithOrdinals) _keyFieldValues).ordinals().getMaxOrd();
            int tsPointer = 0;
            _timestamps.resetQuick();
            _timestamps.add(0);
            _ordToTimestampPointers.resetQuick();
            _ordToTimestampPointers.add(0);
            long lastDateTime = 0;
            long lastTimestamp = 0;
            for(int i = 1; i < maxOrd; i++) {
                final long datetime = ((WithOrdinals) _keyFieldValues).getValueByOrd(i);

                // If this datetime is less than a second after the previously-seen timestamp, it will have the same timestamp
                // (true because we don't support granularity less than 1 sec)
                if(datetime == lastDateTime || (datetime > lastTimestamp && datetime - lastTimestamp < 1000)) {
                    // Just add another instance of the same timestamp pointer
                    _ordToTimestampPointers.add(tsPointer);
                } else {
                    // We may or may not have a new timestamp
                    final long newTimestamp = _tzRounding.calc(datetime);
                    if(newTimestamp != lastTimestamp) {
                        // We do -- save it and update pointer
                        lastTimestamp = newTimestamp;
                        _timestamps.add(newTimestamp);
                        tsPointer++;
                        // Otherwise this ord will have the same pointer as the last one
                    }
                }
                lastDateTime = datetime;
                _ordToTimestampPointers.add(tsPointer);
            }
        } else {
            _docIter = EMPTY;
        }
    }

    @Override
    public void postCollection() {}

    protected boolean hasNextTimestamp() {
        if(_keyFieldValues instanceof WithOrdinals) {
            return _docOrdPointer < _docOrds.length;
        } else {
            return _docIter.hasNext();
        }
    }

    protected long nextTimestamp() {
        if(_keyFieldValues instanceof WithOrdinals) {
            final long ts = _timestamps.get(_ordToTimestampPointers.get(_docOrds.ints[_docOrdPointer]));
            _docOrdPointer++;
            return ts;
        } else {
            final long datetime = _docIter.next();
            // If this datetime is less than a second after the previously-seen timestamp, it will have the same timestamp
            // (true because we don't support granularity less than 1 sec)
            if(datetime == _lastNonOrdDatetime || (datetime > _lastNonOrdTimestamp && datetime - _lastNonOrdTimestamp < 1000)) {
                _lastNonOrdDatetime = datetime;
                // _lastNonOrdTimestamp already contains right value
            } else {
                // Get and save new timestamp
                _lastNonOrdDatetime = datetime;
                _lastNonOrdTimestamp = _tzRounding.calc(datetime);
            }
            return _lastNonOrdTimestamp;
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
