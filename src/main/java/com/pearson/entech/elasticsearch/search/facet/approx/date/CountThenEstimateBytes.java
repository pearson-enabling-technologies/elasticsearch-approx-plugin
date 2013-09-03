package com.pearson.entech.elasticsearch.search.facet.approx.date;

// Based on CountThenEstimate.java from ClearSpring's stream-lib package

import java.io.ByteArrayInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
    protected final static byte LC = 1;
    protected final static byte AC = 2;
    protected final static byte HLC = 3;
    protected final static byte LLC = 4;
    protected final static byte HLPC = 5;

    /**
     * Cardinality after which exact counting gives way to estimation
     */
    private int _tippingPoint;

    /**
     * True after switching to estimation
     */
    protected boolean _tipped = false;

    /**
     * Factory for instantiating _estimator after the tipping point is reached
     */
    protected IBuilder<ICardinality> _builder;

    /**
     * Cardinality _estimator
     * Null until tipping point is reached
     */
    protected ICardinality _estimator;

    /**
     * Cardinality _counter
     * Null after tipping point is reached
     */
    protected BytesRefHash _counter;
    private static final Method __compact = getCompactMethod();
    private static final Object[] __emptyParams = {};
    protected int _totalCounterBytes = 0;

    /**
     *  Used for adding bytesrefs to the _estimator, not used in exact counting
     */
    private static final MurmurHash2 __luceneMurmurHash = MurmurHash2.INSTANCE;

    /**
     * Default constructor
     * Exact counts up to 1000, estimation done with default Builder
     * @param tippingPoint Cardinality at which exact counting gives way to estimation
     * @param builder      Factory for instantiating _estimator after the tipping point is reached
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
     * Deserialization constructor
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

    public boolean offerBytesRefUnsafe(final BytesRef unsafe) {
        boolean modified = false;
        if(_tipped) {
            // The _estimator just needs the hash of the current bytes of the BytesRef
            modified = _estimator.offerHashed(__luceneMurmurHash.hash(unsafe));
        } else {
            // The _counter must copy the BytesRef as it needs to stay intact
            if(_counter.add(BytesRef.deepCopyOf(unsafe)) >= 0) {
                modified = true;
                if(_counter.size() > _tippingPoint) {
                    tip();
                } else {
                    _totalCounterBytes += unsafe.length;
                }
            }
        }
        return modified;
    }

    public boolean offerBytesRefSafe(final BytesRef safe) {
        boolean modified = false;
        if(_tipped) {
            // The _estimator just needs the hash of the current bytes of the BytesRef
            modified = _estimator.offerHashed(__luceneMurmurHash.hash(safe));
        } else {
            if(_counter.add(safe) >= 0) {
                modified = true;
                if(_counter.size() > _tippingPoint) {
                    tip();
                } else {
                    _totalCounterBytes += safe.length;
                }
            }
        }
        return modified;
    }

    @Override
    public boolean offer(final Object o)
    {
        if(o instanceof BytesRef)
            return offerBytesRefUnsafe((BytesRef) o);

        final BytesRef ref = new BytesRef(o.toString());

        boolean modified = false;
        if(_tipped) {
            // The _estimator just needs the hash of the current bytes of the BytesRef
            modified = _estimator.offerHashed(__luceneMurmurHash.hash(ref));
        } else {
            if(_counter.add(ref) >= 0) {
                modified = true;
                if(_counter.size() > _tippingPoint) {
                    tip();
                } else {
                    _totalCounterBytes += ref.length;
                }
            }
        }
        return modified;
    }

    @Override
    public int sizeof() {
        if(_tipped)
            return _estimator.sizeof();

        return -1;
    }

    public int getTippingPoint() {
        return _tippingPoint;
    }

    /**
     * Switch from exact counting to estimation
     */
    private void tip() {
        if(!_tipped) {
            _estimator = _builder.build();
            process(_counter, new Procedure() {
                @Override
                public void consume(final BytesRef unsafe) {
                    _estimator.offerHashed(__luceneMurmurHash.hash(unsafe));
                }
            });
            _counter = null;
            _totalCounterBytes = 0;
            _builder = null;
            _tipped = true;
        }
    }

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

            _totalCounterBytes = in.readInt();

            final byte[] backing = new byte[_totalCounterBytes];
            int backingPointer = 0;

            for(int i = 0; i < count; i++) {
                final int length = in.readInt();
                in.read(backing, backingPointer, length);
                _counter.add(new BytesRef(backing, backingPointer, length));
                backingPointer += length;
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
            out.writeInt(_totalCounterBytes);

            process(_counter, new Procedure() {
                @Override
                public void consume(final BytesRef unsafe) throws IOException {
                    out.writeInt(unsafe.length);
                    out.write(unsafe.bytes, unsafe.offset, unsafe.length);
                }
            });

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
     * Merges estimators to produce an _estimator for their combined streams
     *
     * @param estimators
     * @return merged _estimator or null if no estimators were provided
     * @throws CountThenEstimateMergeException
     *          if estimators are not mergeable (all must be CountThenEstimateBytes made with the same _builder)
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
                        public void consume(final BytesRef unsafe) throws Exception {
                            merged.offerBytesRefUnsafe(unsafe);
                        }
                    });
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

    private static void process(final BytesRefHash hash, final Procedure proc) {
        int[] ids;
        try {
            ids = (int[]) __compact.invoke(hash, __emptyParams);
        } catch(final IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch(final InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        final BytesRef scratch = new BytesRef();
        for(int i = 0; i < ids.length; i++) {
            final int id = ids[i];
            if(id < 0)
                break;
            hash.get(id, scratch);
            try {
                proc.consume(scratch);
            } catch(final Exception e) {
                throw new IllegalStateException(e);
            }
        }
        hash.clear();
    }

    private static interface Procedure {
        void consume(BytesRef unsafe) throws Exception;
    }

    private static Method getCompactMethod() {
        Method compact;
        try {
            compact = BytesRefHash.class.getDeclaredMethod("compact", new Class[0]);
        } catch(final SecurityException e) {
            throw new RuntimeException(e);
        } catch(final NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        compact.setAccessible(true);
        return compact;
    }

    @SuppressWarnings("serial")
    protected static class CountThenEstimateMergeException extends CardinalityMergeException
    {
        public CountThenEstimateMergeException(final String message)
        {
            super(message);
        }
    }

}
