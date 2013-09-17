package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

// Based on CountThenEstimate.java from ClearSpring's stream-lib package

import static com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.process;

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.bloom.MurmurHash2;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.clearspring.analytics.stream.cardinality.LogLog;
import com.clearspring.analytics.util.ExternalizableUtil;
import com.clearspring.analytics.util.IBuilder;
import com.pearson.entech.elasticsearch.plugin.approx.BytesRefUtils.Procedure;

/**
 * Exact -> Estimator cardinality counting
 * <p/>
 * <p>
 * Avoids allocating a large block of memory for cardinality estimation until
 * a specified "tipping point" cardinality is reached.
 * </p>
 * <p/>
 * Currently supports serialization with LinearCounting or AdaptiveCounting
 */
public class CountThenEstimateBytes implements ICardinality, Externalizable
{

    /**
     *  Hash function used when adding bytesrefs to the estimator, not used in exact counting.
     */
    private static final MurmurHash2 __luceneMurmurHash = MurmurHash2.INSTANCE;

    /** Linear Counting constant for serialization. */
    protected final static byte LC = 1;

    /** Adaptive Counting constant for serialization. */
    protected final static byte AC = 2;

    /** HyperLogLog constant for serialization. */
    protected final static byte HLC = 3;

    /** LogLog constant for serialization. */
    protected final static byte LLC = 4;

    /** HyperLogLog Plus constant for serialization. */
    protected final static byte HLPC = 5;

    /**
     * Cardinality after which exact counting gives way to estimation.
     */
    private int _tippingPoint;

    /**
     * True after switching to estimation.
     */
    protected boolean _tipped = false;

    /**
     * True after compacting the counter storage for exact cardinality.
     * You can't add new data after this has been done, so an exception will be thrown.
     */
    protected boolean _compacted = false;

    /**
     * Factory for instantiating estimator after the tipping point is reached
     */
    protected IBuilder<ICardinality> _builder;

    /**
     * Cardinality estimator: null until tipping point is reached
     */
    protected ICardinality _estimator;

    /**
     * Cardinality counter: null after tipping point is reached
     */
    protected BytesRefHash _counter;

    /**
     * Size of the longest BytesRef that the counter object has seen
     */
    protected int _longestBytesRefSize = 0;

    /**
     * Create a new count-then-estimate cardinality object with the tipping point provided.
     * After this has been reached, the provided builder will be used to create an estimator.
     * 
     * @param tippingPoint Cardinality at which exact counting gives way to estimation
     * @param builder      Factory for instantiating estimator after the tipping point is reached
     */
    public CountThenEstimateBytes(final int tippingPoint, final IBuilder<ICardinality> builder) {
        _tippingPoint = tippingPoint;
        _builder = builder;
        if(tippingPoint == 0) {
            _counter = null;
            _estimator = builder.build();
            _tipped = true;
        } else {
            _counter = new BytesRefHash();
        }
    }

    /**
     * Deserialization constructor. Creates a new object from an array of bytes.
     *
     * @param bytes
     * @param tippingPoint Cardinality at which exact counting gives way to estimation
     * @param builder for _estimator to use if there are too many bytes for our liking
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public CountThenEstimateBytes(final byte[] bytes, final int tippingPoint,
            final IBuilder<ICardinality> builder) throws IOException, ClassNotFoundException {
        this(tippingPoint, builder);
        readExternal(new ObjectInputStream(new ByteArrayInputStream(bytes)));

        if(!_tipped && _counter.size() > tippingPoint)
            tip();
    }

    @Override
    public long cardinality() {
        if(_tipped) {
            return _estimator.cardinality();
        }
        return _counter.size();
    }

    @Override
    public boolean offerHashed(final long hashedLong) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(final int hashedInt) {
        throw new UnsupportedOperationException();
    }

    /**
     * Add a BytesRef to the underlying counter or estimator, tipping into approx
     * mode if the tipping point has been reached. Calling this when in exact mode,
     * but after merging or serializing this CountThenEstimateBytes instance, will
     * result in an IllegalStateException. The supplied BytesRef does not need to
     * be made safe by the calling class, as it will be copied on entry.
     * 
     * @param ref the BytesRef to add
     * @return true if an equivalent string of bytes had not previously been offered
     */
    public boolean offerBytesRef(final BytesRef ref) {
        boolean modified = false;
        if(_tipped) {
            // The _estimator just needs the hash of the current bytes of the BytesRef
            modified = _estimator.offerHashed(__luceneMurmurHash.hash(ref));
        } else {
            if(_compacted)
                throw new IllegalStateException("Counter has already been compacted -- cannot add new data");
            if(_counter.add(ref) >= 0) {
                modified = true;
                if(_counter.size() > _tippingPoint) {
                    tip();
                } else {
                    if(ref.length > _longestBytesRefSize)
                        _longestBytesRefSize = ref.length;
                }
            }
        }
        return modified;
    }

    @Override
    public boolean offer(final Object o)
    {
        if(o instanceof BytesRef)
            return offerBytesRef((BytesRef) o);
        else
            return offerBytesRef(new BytesRef(o.toString()));
    }

    @Override
    public int sizeof() {
        if(_tipped)
            return _estimator.sizeof();

        return -1;
    }

    /**
     * Returns the tipping point.
     * 
     * @return the number of entries at which this instance switched from exact to approx mode
     */
    public int getTippingPoint() {
        return _tippingPoint;
    }

    /**
     * Switch from exact counting to estimation.
     */
    private void tip() {
        if(!_tipped) {
            _estimator = _builder.build();
            process(_counter, new Procedure() {
                @Override
                public void consume(final BytesRef ref) {
                    _estimator.offerHashed(__luceneMurmurHash.hash(ref));
                }
            });
            _counter = null;
            _longestBytesRefSize = 0;
            _builder = null;
            _tipped = true;
            _compacted = true;
        }
    }

    /**
     * Check the tipped status.
     * 
     * @return true if this instance is in approx mode
     */
    public boolean tipped() {
        return _tipped;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return ExternalizableUtil.toBytes(this);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        _tipped = in.readBoolean();
        if(_tipped) {

            final byte type = in.readByte();
            final byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);

            switch(type)
            {
            case LC:
                _estimator = new LinearCounting(bytes);
                break;
            case AC:
                _estimator = new AdaptiveCounting(bytes);
                break;
            case HLC:
                _estimator = HyperLogLog.Builder.build(bytes);
                break;
            case HLPC:
                _estimator = HyperLogLogPlus.Builder.build(bytes);
                break;
            case LLC:
                _estimator = new LinearCounting(bytes);
                break;
            default:
                throw new IOException("Unrecognized estimator type: " + type);
            }

        } else {

            _tippingPoint = in.readInt();
            _builder = (IBuilder) in.readObject();
            final int count = in.readInt();

            assert (count <= _tippingPoint) : String.format("Invalid serialization: count (%d) > _tippingPoint (%d)", count, _tippingPoint);

            // Just in case some muppet tries to deserialize into an already-used counter
            if(_compacted || _counter.size() > 0) {
                _counter.clear();
                _counter.reinit();
                _compacted = false;
            }

            _longestBytesRefSize = in.readInt();
            final byte[] scratch = new byte[_longestBytesRefSize];
            for(int i = 0; i < count; i++) {
                final int length = in.readInt();
                in.read(scratch, 0, length);
                _counter.add(new BytesRef(scratch, 0, length));
            }
        }
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeBoolean(_tipped);
        if(_tipped) {

            if(_estimator instanceof LinearCounting)
                out.writeByte(LC);
            else if(_estimator instanceof AdaptiveCounting)
                out.writeByte(AC);
            else if(_estimator instanceof HyperLogLog)
                out.writeByte(HLC);
            else if(_estimator instanceof HyperLogLogPlus)
                out.writeByte(HLPC);
            else if(_estimator instanceof LogLog)
                out.writeByte(LLC);
            else
                throw new IOException("Estimator unsupported for serialization: " + _estimator.getClass().getName());

            final byte[] bytes = _estimator.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);

        } else {

            out.writeInt(_tippingPoint);
            out.writeObject(_builder);
            out.writeInt(_counter.size());
            out.writeInt(_longestBytesRefSize);

            process(_counter, new Procedure() {
                @Override
                public void consume(final BytesRef ref) throws IOException {
                    out.writeInt(ref.length);
                    out.write(ref.bytes, ref.offset, ref.length);
                }
            });
            _compacted = true;

        }
    }

    @Override
    public ICardinality merge(final ICardinality... estimators) throws CardinalityMergeException {
        if(estimators == null)
            return mergeEstimators(this);

        final CountThenEstimateBytes[] all = Arrays.copyOf(estimators, estimators.length + 1, CountThenEstimateBytes[].class);
        all[all.length - 1] = this;
        return mergeEstimators(all);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams.
     *
     * @param estimators the estimators to merge
     * @return a merged estimator, or null if no estimators were provided
     * @throws CardinalityMergeException
     *          if estimators are not mergeable (all must be CountThenEstimateBytes made with the same builder)
     */
    public static CountThenEstimateBytes mergeEstimators(final CountThenEstimateBytes... estimators) throws CardinalityMergeException
    {
        final CountThenEstimateBytes merged;
        final int numEstimators = (estimators == null) ? 0 : estimators.length;
        if(numEstimators > 0) {
            final List<ICardinality> tipped = new ArrayList<ICardinality>(numEstimators);
            final List<CountThenEstimateBytes> untipped = new ArrayList<CountThenEstimateBytes>(numEstimators);

            for(final CountThenEstimateBytes estimator : estimators) {
                if(estimator._tipped)
                    tipped.add(estimator._estimator);
                else
                    untipped.add(estimator);
            }

            final int untippedSize = untipped.size();
            if(untippedSize > 0) {

                merged = untipped.get(0);
                for(int i = 1; i < untippedSize; i++) {
                    final CountThenEstimateBytes cte = untipped.get(i);
                    process(cte._counter, new Procedure() {
                        @Override
                        public void consume(final BytesRef ref) throws Exception {
                            merged.offerBytesRef(ref);
                        }
                    });
                    cte._compacted = true;
                }

            } else {

                merged = new CountThenEstimateBytes(0, new LinearCounting.Builder(1));
                merged.tip();
                merged._estimator = tipped.remove(0);

            }

            if(!tipped.isEmpty()) {
                if(!merged._tipped)
                    merged.tip();

                merged._estimator = merged._estimator.merge(tipped.toArray(new ICardinality[tipped.size()]));
            }

            return merged;

            // TODO we need to make sure that after the final merge, i.e. in the materialize phase, all remaining memory is cleared
        }
        return null;
    }

    /**
     * Exception thrown to indicate that you have asked to merge two incompatible estimators.
     */
    @SuppressWarnings("serial")
    protected static class CountThenEstimateMergeException extends CardinalityMergeException
    {

        /**
         * Create exception.
         * @param message
         */
        public CountThenEstimateMergeException(final String message)
        {
            super(message);
        }

    }

}
