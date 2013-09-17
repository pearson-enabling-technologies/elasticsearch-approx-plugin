package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.map.TLongObjectMap;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder;

/**
 * Essentially a wrapper for CountThenEstimateBytes, providing some defaults
 * and utility functions. In approximate mode, it uses HyperLogLog estimation
 * with a relative standard deviation of 0.0025 -- this is currently hardcoded.
 * The tipping point from exact->approx is configurable. It also keeps track
 * of the total number of updates, i.e. the non-distinct count.
 * 
 * TODO document how much memory the estimator uses
 */
public class DistinctCountPayload {

    /**
     * Standard builder for HyperLogLog estimators.
     */
    private final Builder _stdBuilder = new HyperLogLog.Builder(0.0025);

    /**
     * The total number of updates (the non-distinct count).
     */
    private long _count;

    /**
     * A cardinality estimator that can operate in exact or distinct mode.
     */
    private CountThenEstimateBytes _cardinality;

    /**
     * Create a new payload object.
     * 
     * @param entryLimit the number of entries to gather in exact mode before changing to approx mode
     */
    public DistinctCountPayload(final int entryLimit) {
        _count = 0;
        // FIXME defer creation of this until we actually need it
        _cardinality = new CountThenEstimateBytes(entryLimit, _stdBuilder);
    }

    /**
     * Deserialization constructor. The inverse of the writeTo() method.
     * 
     * @param in an ElasticSearch StreamInput object to read data from
     * @throws IOException
     */
    DistinctCountPayload(final StreamInput in) throws IOException {
        _count = in.readVLong();
        final int entryLimit = in.readVInt();
        final int payloadSize = in.readVInt();
        final byte[] payloadBytes = new byte[payloadSize];
        in.readBytes(payloadBytes, 0, payloadSize);
        try {
            _cardinality = new CountThenEstimateBytes(payloadBytes, entryLimit, _stdBuilder);
        } catch(final ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /**
     * Add a new value. The BytesRef holding the value need not be "safe"
     * as it will be copied on adding.
     * 
     * @param ref a BytesRef holding the value
     * @return true if this value has not been previously added
     */
    public boolean update(final BytesRef ref) {
        _count++;
        return _cardinality.offerBytesRef(ref);
    }

    /**
     * Get the total (non-distinct) count -- the number of times this payload
     * has been updated.
     * 
     * @return the count
     */
    public long getCount() {
        return _count;
    }

    /**
     * Get the distinct count (either exact or approximate depending on
     * whether the underlying cardinality object has tipped into approx mode).
     * 
     * @return the distinct count
     */
    public long getDistinctCount() {
        return _cardinality.cardinality();
    }

    /**
     * Merge another payload into this one. The other one will be cleared by this process.
     * 
     * @param other the payload to merge
     * @return this payload, now containing merged data
     * @throws CardinalityMergeException
     */
    DistinctCountPayload merge(final DistinctCountPayload other) throws CardinalityMergeException {
        _count += other._count;
        _cardinality = CountThenEstimateBytes.mergeEstimators(this._cardinality, other._cardinality);
        return this;
    }

    /**
     * Merge this payload into a map of longs (e.g. timestamps) to payloads, by
     * merging it into an existing payload with the supplied key if available,
     * or adding it with the supplied key if not. N.B. The object upon which you
     * called mergeInto() may not be safe to use afterwards as it may have been
     * cleared; use the returned object instead.
     *  
     * @param map the map to merge into
     * @param key the key of the entry which is the target of the merge
     * @return the resulting merged payload
     */
    DistinctCountPayload mergeInto(final TLongObjectMap<DistinctCountPayload> map, final long key) {
        final DistinctCountPayload other = map.get(key);
        if(other == null) {
            map.put(key, this);
            return this;
        } else
            try {
                other.merge(this);
                return other;
            } catch(final CardinalityMergeException e) {
                throw new ElasticSearchException("Unable to merge two facet cardinality objects", e);
            }
    }

    /**
     * Merge this payload into a map of objects to payloads, by
     * merging it into an existing payload with the supplied key if available,
     * or adding it with the supplied key if not. N.B. The object upon which you
     * called mergeInto() may not be safe to use afterwards as it may have been
     * cleared; use the returned object instead.
     * 
     * @param <K> the type of the map key
     * @param map the map to merge into
     * @param key the key of the entry which is the target of the merge
     * @return the resulting merged payload
     */
    <K> DistinctCountPayload mergeInto(final ExtTHashMap<K, DistinctCountPayload> map, final K key) {
        if(map.containsKey(key))
            try {
                map.put(key, this.merge(map.get(key)));
            } catch(final CardinalityMergeException e) {
                throw new ElasticSearchException("Unable to merge two facet cardinality objects", e);
            }
        else
            map.put(key, this);
        return this;
    }

    @Override
    public String toString() {
        final String descr = _cardinality.sizeof() == -1 ?
                "Set" : "Estimator";
        return String.format(
                "%s of %d distinct elements (%d total elements)",
                descr, _cardinality.cardinality(), _count);
    }

    /**
     * Serialize this payload to an ElasticSearch StreamOutput object.
     * The inverse of the deserialization constructor.
     * 
     * @param output the StreamOutput
     * @throws IOException
     */
    public void writeTo(final StreamOutput output) throws IOException {
        output.writeVLong(_count);
        output.writeVInt(_cardinality.getTippingPoint());
        final byte[] bytes = _cardinality.getBytes();
        output.writeVInt(bytes.length);
        output.writeBytes(bytes);
    }

}