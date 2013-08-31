package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.Arrays;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.map.hash.TLongLongHashMap;

public class TimeZoneRoundingCache {

    // TODO can we actually cache TimeZoneRounding objects too?
    // TODO how about an object pool for long[] -- or extend CacheRecycler

    private final TimeZoneRounding _tzRounding;
    private final int _maxSize;
    private final TLongLongHashMap _cache;
    private final long _noEntryKey;
    private final long[] _keyRing;
    private int _keyRingPointer;

    public TimeZoneRoundingCache(final TimeZoneRounding tzRounding, final int maxSize) {
        _tzRounding = tzRounding;
        _maxSize = maxSize;
        _cache = CacheRecycler.popLongLongMap();
        _noEntryKey = _cache.getNoEntryKey();
        _keyRing = new long[maxSize];
        Arrays.fill(_keyRing, _noEntryKey);
        _keyRingPointer = 0;
    }

    public void trash() {
        CacheRecycler.pushLongLongMap(_cache);
    }

    public int occupancy() {
        return _cache.size();
    }

    public long round(final long timestamp) {
        final long rounded = _cache.get(timestamp);
        if(rounded == _noEntryKey)
            return roundAndCache(timestamp);
        else
            return rounded;
    }

    private long roundAndCache(final long timestamp) {
        final long rounded = _tzRounding.calc(timestamp);
        put(timestamp, rounded);
        return rounded;
    }

    private void put(final long key, final long value) {
        if(_cache.size() < _maxSize) {
            // Just store the value, advance the pointer to the next in the keyring, and save the key there
            _cache.put(key, value);
            advancePointer();
            _keyRing[_keyRingPointer] = key;
        } else {
            // Advance the pointer, remove the corresponding value from the cache, store the new value, and store the pointer
            advancePointer();
            _cache.remove(_keyRing[_keyRingPointer]);
            _cache.put(key, value);
            _keyRing[_keyRingPointer] = key;
            assert _cache.size() <= _maxSize; // TODO remove me after testing
        }
    }

    private void advancePointer() {
        final int newPointer = _keyRingPointer++;
        if(newPointer == _maxSize)
            _keyRingPointer = 0;
        else
            _keyRingPointer = newPointer;
    }

}
