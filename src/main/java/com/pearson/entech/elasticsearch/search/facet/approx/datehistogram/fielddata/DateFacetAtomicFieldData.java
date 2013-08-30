/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.fielddata;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

/**
 */
public abstract class DateFacetAtomicFieldData extends AtomicNumericFieldData {

    public static final DateFacetAtomicFieldData EMPTY = new Empty();

    protected final int[] values;
    private final int numDocs;

    protected long size = -1;

    public DateFacetAtomicFieldData(final int[] values, final int numDocs) {
        super(false);
        this.values = values;
        this.numDocs = numDocs;
    }

    @Override
    public void close() {}

    @Override
    public int getNumDocs() {
        return numDocs;
    }

    static class Empty extends DateFacetAtomicFieldData {

        Empty() {
            super(null, 0);
        }

        @Override
        public LongValues getLongValues() {
            return LongValues.EMPTY;
        }

        @Override
        public DoubleValues getDoubleValues() {
            return DoubleValues.EMPTY;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getMemorySizeInBytes() {
            return 0;
        }

        @Override
        public BytesValues getBytesValues() {
            return BytesValues.EMPTY;
        }

        @Override
        public ScriptDocValues getScriptValues() {
            return ScriptDocValues.EMPTY;
        }
    }

    public static class WithOrdinals extends DateFacetAtomicFieldData {

        private final Ordinals ordinals;

        public WithOrdinals(final int[] values, final int numDocs, final Ordinals ordinals) {
            super(values, numDocs);
            this.ordinals = ordinals;
        }

        @Override
        public boolean isMultiValued() {
            return ordinals.isMultiValued();
        }

        @Override
        public boolean isValuesOrdered() {
            return true;
        }

        @Override
        public long getMemorySizeInBytes() {
            if(size == -1) {
                size = RamUsage.NUM_BYTES_INT/*size*/+ RamUsage.NUM_BYTES_INT/*numDocs*/+ +RamUsage.NUM_BYTES_ARRAY_HEADER
                        + (values.length * RamUsage.NUM_BYTES_INT) + ordinals.getMemorySizeInBytes();
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, ordinals.ordinals());
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, ordinals.ordinals());
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues.WithOrdinals {

            private final int[] values;

            LongValues(final int[] values, final Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public long getValueByOrd(final int ord) {
                return values[ord] * 1000;
            }

        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.WithOrdinals {

            private final int[] values;

            DoubleValues(final int[] values, final Ordinals.Docs ordinals) {
                super(ordinals);
                this.values = values;
            }

            @Override
            public double getValueByOrd(final int ord) {
                return values[ord] * 1000;
            }

        }
    }

    /**
     * A single valued case, where not all values are "set", so we have a FixedBitSet that
     * indicates which values have an actual value.
     */
    public static class SingleFixedSet extends DateFacetAtomicFieldData {

        private final FixedBitSet set;

        public SingleFixedSet(final int[] values, final int numDocs, final FixedBitSet set) {
            super(values, numDocs);
            this.set = set;
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getMemorySizeInBytes() {
            if(size == -1) {
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_INT) + (set.getBits().length * RamUsage.NUM_BYTES_LONG);
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values, set);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values, set);
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

            private final int[] values;
            private final FixedBitSet set;

            LongValues(final int[] values, final FixedBitSet set) {
                super(false);
                this.values = values;
                this.set = set;
            }

            @Override
            public boolean hasValue(final int docId) {
                return set.get(docId);
            }

            @Override
            public long getValue(final int docId) {
                return values[docId] * 1000;
            }

        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

            private final int[] values;
            private final FixedBitSet set;

            DoubleValues(final int[] values, final FixedBitSet set) {
                super(false);
                this.values = values;
                this.set = set;
            }

            @Override
            public boolean hasValue(final int docId) {
                return set.get(docId);
            }

            @Override
            public double getValue(final int docId) {
                return values[docId] * 1000;
            }
        }
    }

    /**
     * Assumes all the values are "set", and docId is used as the index to the value array.
     */
    public static class Single extends DateFacetAtomicFieldData {

        /**
         * Note, here, we assume that there is no offset by 1 from docId, so position 0
         * is the value for docId 0.
         */
        public Single(final int[] values, final int numDocs) {
            super(values, numDocs);
        }

        @Override
        public boolean isMultiValued() {
            return false;
        }

        @Override
        public boolean isValuesOrdered() {
            return false;
        }

        @Override
        public long getMemorySizeInBytes() {
            if(size == -1) {
                size = RamUsage.NUM_BYTES_ARRAY_HEADER + (values.length * RamUsage.NUM_BYTES_INT);
            }
            return size;
        }

        @Override
        public LongValues getLongValues() {
            return new LongValues(values);
        }

        @Override
        public DoubleValues getDoubleValues() {
            return new DoubleValues(values);
        }

        static class LongValues extends org.elasticsearch.index.fielddata.LongValues.Dense {

            private final int[] values;

            LongValues(final int[] values) {
                super(false);
                assert values.length != 0;
                this.values = values;
            }

            @Override
            public long getValue(final int docId) {
                return values[docId] * 1000;
            }

        }

        static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues.Dense {

            private final int[] values;

            DoubleValues(final int[] values) {
                super(false);
                this.values = values;
            }

            @Override
            public double getValue(final int docId) {
                return values[docId] * 1000;
            }
        }

    }
}
