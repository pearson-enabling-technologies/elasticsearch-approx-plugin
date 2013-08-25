package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.codecs.bloom.HashFunction;
import org.apache.lucene.codecs.bloom.MurmurHash2;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
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
    protected THashSet<BytesRef> counter;

    /**
     * Default constructor
     * Exact counts up to 1000, estimation done with default Builder
     */
    public CountThenEstimateBytes()
    {
        this(1000, AdaptiveCounting.Builder.obyCount(1000000000));
    }

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
            this.counter = CacheRecycler.popHashSet();
            // Pre-allocate space for hash, with a sensible cutoff
            final int initialCapacity = min(tippingPoint, 10000);
            this.counter.ensureCapacity(initialCapacity);
        }
    }

    /**
     * Deserialization constructor
     *
     * @param bytes
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public CountThenEstimateBytes(final byte[] bytes) throws IOException, ClassNotFoundException
    {
        readExternal(new ObjectInputStream(new ByteArrayInputStream(bytes)));

        if(!tipped && builder.sizeof() <= bytes.length)
        {
            tip();
        }
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

    public boolean offerBytesRef(final BytesRef unsafe) {
        boolean modified = false;

        if(tipped)
        {
            // The estimator just needs the hash of the current bytes of the BytesRef
            modified = estimator.offerHashed(__luceneMurmurHash.hash(unsafe));
        }
        else
        {
            // The counter must copy the BytesRef as it needs to stay intact
            if(counter.add(BytesRef.deepCopyOf(unsafe)))
            {
                modified = true;
                if(counter.size() > tippingPoint)
                {
                    tip();
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

        final BytesRef ref = new BytesRef(o.toString());

        boolean modified = false;

        if(tipped)
        {
            // The estimator just needs the hash of the current bytes of the BytesRef
            modified = estimator.offerHashed(__luceneMurmurHash.hash(ref));
        }
        else
        {
            if(counter.add(ref))
            {
                modified = true;
                if(counter.size() > tippingPoint)
                {
                    tip();
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
            _offerMembers.init(estimator, __luceneMurmurHash);
            counter.forEach(_offerMembers);
            counter = null;
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

            counter = CacheRecycler.popHashSet();
            for(int i = 0; i < count; i++)
            {
                final int length = in.readInt();
                final byte[] bytes = new byte[length];
                in.readFully(bytes);
                counter.add(new BytesRef(bytes));
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
            for(final BytesRef o : counter)
            {
                out.writeInt(o.length);
                out.write(o.bytes, o.offset, o.length);
            }
            CacheRecycler.pushHashSet(counter);
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
        CountThenEstimateBytes merged = null;
        final int numEstimators = (estimators == null) ? 0 : estimators.length;
        if(numEstimators > 0)
        {
            final List<ICardinality> tipped = new ArrayList<ICardinality>(numEstimators);
            final List<CountThenEstimateBytes> untipped = new ArrayList<CountThenEstimateBytes>(numEstimators);

            for(final CountThenEstimateBytes estimator : estimators)
            {
                if(estimator.tipped)
                {
                    tipped.add(estimator.estimator);
                }
                else
                {
                    untipped.add(estimator);
                }
            }

            if(untipped.size() > 0)
            {
                merged = new CountThenEstimateBytes(untipped.get(0).tippingPoint, untipped.get(0).builder);

                for(final CountThenEstimateBytes cte : untipped)
                {
                    for(final Object o : cte.counter)
                    {
                        merged.offerBytesRef((BytesRef) o);
                    }
                }
            }
            else
            {
                merged = new CountThenEstimateBytes(0, new LinearCounting.Builder(1));
                merged.tip();
                merged.estimator = tipped.remove(0);
            }

            if(!tipped.isEmpty())
            {
                if(!merged.tipped)
                {
                    merged.tip();
                }
                merged.estimator = merged.estimator.merge(tipped.toArray(new ICardinality[tipped.size()]));
            }

        }
        return merged;
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

    }

}
