package com.pearson.entech.elasticsearch.search.facet.approx.date;

/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static java.lang.Math.min;

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
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.codecs.bloom.HashFunction;
import org.apache.lucene.codecs.bloom.MurmurHash2;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.trove.procedure.TObjectProcedure;
import org.elasticsearch.common.trove.set.hash.THashSet;

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
    protected int tippingPoint;

    /**
     * True after switching to estimation
     */
    protected boolean tipped = false;

    /**
     * Factory for instantiating estimator after the tipping point is reached
     */
    protected IBuilder<ICardinality> builder;

    /**
     * Cardinality estimator
     * Null until tipping point is reached
     */
    protected ICardinality estimator;

    /**
     * Cardinality counter
     * Null after tipping point is reached
     */
    protected BytesRefHash counter;
    private static final Method __compact = getCompactMethod();
    private static final Class<?>[] __emptyParamTypes = {};
    private static final Object[] __emptyParams = {};
    protected int totalCounterBytes = 0;

    /**
     * Default constructor
     * Exact counts up to 1000, estimation done with default Builder
     */
    //    public CountThenEstimateBytes()
    //    {
    //        this(1000, AdaptiveCounting.Builder.obyCount(1000000000));
    //    }

    // Used for adding bytesrefs to the estimator, not used in exact counting
    private static final MurmurHash2 __luceneMurmurHash = MurmurHash2.INSTANCE;

    /**
     * @param tippingPoint Cardinality at which exact counting gives way to estimation
     * @param builder      Factory for instantiating estimator after the tipping point is reached
     */
    public CountThenEstimateBytes(final int tippingPoint, final IBuilder<ICardinality> builder)
    {
        this.tippingPoint = tippingPoint;
        this.builder = builder;
        if(tippingPoint == 0) {
            this.counter = null;
            this.estimator = builder.build();
            this.tipped = true;
        } else {
            //            this.counter = CacheRecycler.popHashSet();
            // Pre-allocate space for hash, with a sensible cutoff
            final int initialCapacity = min(tippingPoint, 10000);
            //            this.counter.ensureCapacity(initialCapacity);
            this.counter = new BytesRefHash();
        }
    }

    /**
     * Deserialization constructor
     *
     * @param bytes
     * @param ints
     * @param builder for estimator to use if there are too many bytes for our liking
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public CountThenEstimateBytes(final byte[] bytes, final int tippingPoint,
            final IBuilder<ICardinality> builder) throws IOException, ClassNotFoundException
    {
        this(tippingPoint, builder);
        readExternal(new ObjectInputStream(new ByteArrayInputStream(bytes)));

        if(!tipped && counter.size() > tippingPoint)
            tip();
    }

    @Override
    public long cardinality()
    {
        if(tipped)
        {
            return estimator.cardinality();
        }
        return counter.size();
    }

    @Override
    public boolean offerHashed(final long hashedLong)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offerHashed(final int hashedInt)
    {
        throw new UnsupportedOperationException();
    }

    public boolean offerBytesRefUnsafe(final BytesRef unsafe) {
        boolean modified = false;

        if(tipped)
        {
            // The estimator just needs the hash of the current bytes of the BytesRef
            modified = estimator.offerHashed(__luceneMurmurHash.hash(unsafe));
        }
        else
        {
            // The counter must copy the BytesRef as it needs to stay intact
            if(counter.add(BytesRef.deepCopyOf(unsafe)) >= 0)
            {
                modified = true;
                if(counter.size() > tippingPoint)
                {
                    tip();
                }
                else
                {
                    totalCounterBytes += unsafe.length;
                }
            }
        }

        return modified;
    }

    public boolean offerBytesRefSafe(final BytesRef safe) {
        boolean modified = false;

        if(tipped)
        {
            // The estimator just needs the hash of the current bytes of the BytesRef
            modified = estimator.offerHashed(__luceneMurmurHash.hash(safe));
        }
        else
        {
            if(counter.add(safe) >= 0)
            {
                modified = true;
                if(counter.size() > tippingPoint)
                {
                    tip();
                }
                else
                {
                    totalCounterBytes += safe.length;
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

        if(tipped)
        {
            // The estimator just needs the hash of the current bytes of the BytesRef
            modified = estimator.offerHashed(__luceneMurmurHash.hash(ref));
        }
        else
        {
            if(counter.add(ref) >= 0)
            {
                modified = true;
                if(counter.size() > tippingPoint)
                {
                    tip();
                }
                else
                {
                    totalCounterBytes += ref.length;
                }
            }
        }

        return modified;
    }

    @Override
    public int sizeof()
    {
        if(tipped)
        {
            return estimator.sizeof();
        }
        return -1;
    }

    /**
     * Switch from exact counting to estimation
     */
    private void tip()
    {
        if(!tipped) {
            estimator = builder.build();
            //            _offerMembers.init(estimator, __luceneMurmurHash);
            //            counter.forEach(_offerMembers);
            //            _offerMembers.clear();

            process(counter, new Procedure() {
                @Override
                public void consume(final BytesRef unsafe) {
                    estimator.offerHashed(__luceneMurmurHash.hash(unsafe));
                }
            });

            counter = null;
            totalCounterBytes = 0;
            builder = null;
            tipped = true;
        }
    }

    public boolean tipped()
    {
        return tipped;
    }

    @Override
    public byte[] getBytes() throws IOException
    {
        return ExternalizableUtil.toBytes(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException
    {
        tipped = in.readBoolean();
        if(tipped)
        {
            final byte type = in.readByte();
            final byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);

            switch(type)
            {
            case LC:
                estimator = new LinearCounting(bytes);
                break;
            case AC:
                estimator = new AdaptiveCounting(bytes);
                break;
            case HLC:
                estimator = HyperLogLog.Builder.build(bytes);
                break;
            case HLPC:
                estimator = HyperLogLogPlus.Builder.build(bytes);
                break;
            case LLC:
                estimator = new LinearCounting(bytes);
                break;
            default:
                throw new IOException("Unrecognized estimator type: " + type);
            }
        }
        else
        {
            tippingPoint = in.readInt();
            builder = (IBuilder) in.readObject();
            final int count = in.readInt();

            assert (count <= tippingPoint) : String.format("Invalid serialization: count (%d) > tippingPoint (%d)", count, tippingPoint);

            totalCounterBytes = in.readInt();

            final byte[] backing = new byte[totalCounterBytes];
            int backingPointer = 0;

            //            counter = CacheRecycler.popHashSet();
            for(int i = 0; i < count; i++)
            {
                final int length = in.readInt();
                in.read(backing, backingPointer, length);
                counter.add(new BytesRef(backing, backingPointer, length));
                backingPointer += length;
            }
        }
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException
    {
        out.writeBoolean(tipped);
        if(tipped)
        {
            if(estimator instanceof LinearCounting)
            {
                out.writeByte(LC);
            }
            else if(estimator instanceof AdaptiveCounting)
            {
                out.writeByte(AC);
            }
            else if(estimator instanceof HyperLogLog)
            {
                out.writeByte(HLC);
            }
            else if(estimator instanceof HyperLogLogPlus)
            {
                out.writeByte(HLPC);
            }
            else if(estimator instanceof LogLog)
            {
                out.writeByte(LLC);
            }
            else
            {
                throw new IOException("Estimator unsupported for serialization: " + estimator.getClass().getName());
            }

            final byte[] bytes = estimator.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        else
        {
            out.writeInt(tippingPoint);
            out.writeObject(builder);
            out.writeInt(counter.size());
            out.writeInt(totalCounterBytes);
            //            _serializer.init(counter, out);
            //            counter.forEach(_serializer);
            //            _serializer.clear();
            //            CacheRecycler.pushHashSet(counter);

            process(counter, new Procedure() {
                @Override
                public void consume(final BytesRef unsafe) throws IOException {
                    out.writeInt(unsafe.length);
                    out.write(unsafe.bytes, unsafe.offset, unsafe.length);
                }
            });
        }
    }

    @Override
    public ICardinality merge(final ICardinality... estimators) throws CardinalityMergeException
    {
        if(estimators == null)
        {
            return mergeEstimators(this);
        }

        final CountThenEstimateBytes[] all = Arrays.copyOf(estimators, estimators.length + 1, CountThenEstimateBytes[].class);
        all[all.length - 1] = this;
        return mergeEstimators(all);
    }

    /**
     * Merges estimators to produce an estimator for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     * @throws CountThenEstimateMergeException
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
                if(estimator.tipped) {
                    tipped.add(estimator.estimator);
                } else {
                    untipped.add(estimator);
                }
            }

            final int untippedSize = untipped.size();
            if(untippedSize > 0) {
                //                merged = new CountThenEstimateBytes(untipped.get(0).tippingPoint, untipped.get(0).builder);
                merged = untipped.get(0);

                for(int i = 1; i < untippedSize; i++) {
                    final CountThenEstimateBytes cte = untipped.get(i);
                    process(cte.counter, new Procedure() {
                        @Override
                        public void consume(final BytesRef unsafe) throws Exception {
                            merged.offerBytesRefUnsafe(unsafe);
                        }
                    });

                    //                    for(final Object o : cte.counter)
                    //                    {
                    //                        merged.offerBytesRefSafe((BytesRef) o);
                    //                    }
                }

            } else {

                merged = new CountThenEstimateBytes(0, new LinearCounting.Builder(1));
                merged.tip();
                merged.estimator = tipped.remove(0);

            }

            if(!tipped.isEmpty()) {
                if(!merged.tipped) {
                    merged.tip();
                }
                merged.estimator = merged.estimator.merge(tipped.toArray(new ICardinality[tipped.size()]));
            }

            return merged;
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

    private final MemberOfferer _offerMembers = new MemberOfferer();

    private static class MemberOfferer implements TObjectProcedure<BytesRef> {

        private ICardinality _estimator;
        private HashFunction _hash;

        private void init(final ICardinality estimator, final HashFunction hash) {
            _estimator = estimator;
            _hash = hash;
        }

        @Override
        public boolean execute(final BytesRef bytes) {
            _estimator.offerHashed(_hash.hash(bytes));
            return true;
        }

        public void clear() {
            _estimator = null;
            _hash = null;
        }

    }

    private final HashSetSerializer _serializer = new HashSetSerializer();

    private static class HashSetSerializer implements TObjectProcedure<BytesRef> {

        private THashSet<BytesRef> _counter;
        private ObjectOutput _out;

        private void init(final THashSet<BytesRef> counter, final ObjectOutput out) {
            _counter = counter;
            _out = out;
        }

        @Override
        public boolean execute(final BytesRef bytes) {
            try {
                _out.writeInt(bytes.length);
                _out.write(bytes.bytes, bytes.offset, bytes.length);
            } catch(final IOException e) {
                throw new IllegalStateException(e);
            }
            return true;
        }

        private void clear() {
            _counter = null;
            _out = null;
        }

    }

    private final static NonSortingComparator __nonSorter = new NonSortingComparator();

    private static class NonSortingComparator implements Comparator<BytesRef> {

        @Override
        public int compare(final BytesRef arg0, final BytesRef arg1) {
            return 0;
        }

    }

}
