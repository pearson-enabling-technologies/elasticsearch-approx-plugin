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

/**
 * A buildable collector which iterates through value of a long datetime field, applying timezone rounding to them.
 *  
 * @param <V> the IndexFieldData type of the datetime field 
 */
public abstract class TimestampFirstCollector<V extends AtomicFieldData<? extends ScriptDocValues>> extends BuildableCollector {

    /**
     * An empty iterator over long values. 
     */
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

    /**
     * Create a new collector.
     * 
     * @param keyFieldData key (datetime) field data
     * @param valueFieldData value field data
     * @param tzRounding time zone rounding
     */
    public TimestampFirstCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<V> valueFieldData, final TimeZoneRounding tzRounding) {
        _keyFieldData = keyFieldData;
        _valueFieldData = valueFieldData;
        _tzRounding = tzRounding;
    }

    /**
     * Create a new collector.
     * 
     * @param keyFieldData key (datetime) field data
     * @param tzRounding time zone rounding
     */
    public TimestampFirstCollector(final LongArrayIndexFieldData keyFieldData,
            final TimeZoneRounding tzRounding) {
        this(keyFieldData, null, tzRounding);
    }

    @Override
    public void collect(final int doc) throws IOException {
        // If the datetime field has ordinals available, we can take a bunch of shortcuts later
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

        // If we have ordinals avilable, we can do most of the work up front.
        // We build a mapping from ords to rounded timestamps, so we never
        // have to retrieve the field values for a given document. We just
        // see which ordinals it has and then get the rounded timestamps they
        // correspond to.

        // One drawback of this approach is that if we have a very aggressively
        // filtered query, there might be many ordinals which are never used by
        // any of the documents we will be looking at. So we'd be wasting effort
        // by calculating timestamps for all of the ordinals up front.
        // TODO come up with a heuristic to avoid falling into this trap.

        if(_keyFieldValues instanceof WithOrdinals) {
            final int maxOrd = ((WithOrdinals) _keyFieldValues).ordinals().getMaxOrd();
            int tsPointer = 0;

            // _timestamps holds the rounded timestamps
            _timestamps.resetQuick();
            _timestamps.add(0);

            // _ordToTimestampPointers has one entry for every ord
            _ordToTimestampPointers.resetQuick();
            _ordToTimestampPointers.add(0);

            // We cache these for some small optimizations
            long lastDateTime = 0;
            long lastTimestamp = 0;
            for(int i = 1; i < maxOrd; i++) {
                // Get the next ordinal's value so we can calculate its timestamp
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

                // Add timestamp pointer for this ord -- could be the same as the previous ord, or a new one
                _ordToTimestampPointers.add(tsPointer);
            }
        } else {
            _docIter = EMPTY;
        }
    }

    @Override
    public void postCollection() {}

    /**
     * Are there any more timestamps available?
     * 
     * @return true/false
     */
    protected boolean hasNextTimestamp() {
        if(_keyFieldValues instanceof WithOrdinals) {
            return _docOrdPointer < _docOrds.length;
        } else {
            return _docIter.hasNext();
        }
    }

    /**
     * Get the next timestamp, i.e. the rounded value of the next available datetime.
     * 
     * @return the timestamp
     */
    protected long nextTimestamp() {
        if(_keyFieldValues instanceof WithOrdinals) {
            // We can bypass getting the raw datetime value, and go from ord to timestamp directly (well, directly-ish)
            final long ts = _timestamps.get(_ordToTimestampPointers.get(_docOrds.ints[_docOrdPointer]));
            _docOrdPointer++;
            return ts;
        } else {
            // Get the next raw datetime, and if necessary, round it
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

    /**
     * Returns true if this iterator is getting each timestamp once per value of a value field.
     * Otherwise, it's getting each timestamp once per document.
     * 
     * @return true/false
     */
    protected boolean hasValueField() {
        return _valueFieldData != null;
    }

    /**
     * Returns true if there is another value of a value field available, for the current doc.
     * If there isn't, or we're not using a value field, returns false.
     * 
     * @return true/false
     */
    protected boolean hasNextValue() {
        return _valueFieldIter != null && _valueFieldIter.hasNext();
    }

    /**
     * Gets the next value of the value field, or null if we're not using a value field.
     * 
     * @return the next value as a BytesRef, or null
     */
    protected BytesRef nextValue() {
        return _valueFieldIter == null ? null : _valueFieldIter.next();
    }

}
