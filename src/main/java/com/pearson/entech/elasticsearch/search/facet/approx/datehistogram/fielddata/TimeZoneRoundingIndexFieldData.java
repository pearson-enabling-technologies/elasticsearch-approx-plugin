package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.fielddata;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.list.array.TIntArrayList;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AbstractIndexFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.IntValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.fielddata.plain.IntArrayAtomicFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

public class TimeZoneRoundingIndexFieldData extends AbstractIndexFieldData<AtomicNumericFieldData> implements IndexNumericFieldData<AtomicNumericFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        private final TimeZoneRounding _tzRounding;

        public Builder(final TimeZoneRounding tzRounding) {
            _tzRounding = tzRounding;
        }

        @Override
        public IndexFieldData build(final Index index, @IndexSettings final Settings indexSettings, final FieldMapper.Names fieldNames,
                final FieldDataType type, final IndexFieldDataCache cache) {
            return new TimeZoneRoundingIndexFieldData(index, indexSettings, fieldNames, type, cache, _tzRounding);
        }
    }

    private final TimeZoneRounding _tzRounding;

    public TimeZoneRoundingIndexFieldData(final Index index, @IndexSettings final Settings indexSettings, final FieldMapper.Names fieldNames,
            final FieldDataType fieldDataType, final IndexFieldDataCache cache, final TimeZoneRounding tzRounding) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        _tzRounding = tzRounding;
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.LONG;
    }

    @Override
    public boolean valuesOrdered() {
        // because we might have single values? we can dynamically update a flag to reflect that
        // based on the atomic field data loaded
        return false;
    }

    // TODO store the rounded datetimes as ints (divide them by 1000) and change them back to longs on retrieval

    @Override
    public AtomicNumericFieldData load(final AtomicReaderContext context) {
        try {
            return cache.load(context, this);
        } catch(final Throwable e) {
            if(e instanceof ElasticSearchException) {
                throw (ElasticSearchException) e;
            } else {
                throw new ElasticSearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public AtomicNumericFieldData loadDirect(final AtomicReaderContext context) throws Exception {
        final AtomicReader reader = context.reader();
        final Terms terms = reader.terms(getFieldNames().indexName());
        if(terms == null) {
            return IntArrayAtomicFieldData.EMPTY;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        final TIntArrayList values = new TIntArrayList();

        values.add(0); // first "t" indicates null value
        final OrdinalsBuilder builder = new OrdinalsBuilder(terms, reader.maxDoc());
        try {
            BytesRef term;
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            final BytesRefIterator iter = builder.buildFromTerms(builder.wrapNumeric32Bit(terms.iterator(null)), reader.getLiveDocs());
            while((term = iter.next()) != null) {
                final int value = NumericUtils.prefixCodedToInt(term);
                values.add(value);
                if(value > max) {
                    max = value;
                }
                if(value < min) {
                    min = value;
                }
            }

            final Ordinals build = builder.build(fieldDataType.getSettings());

            return build(reader, fieldDataType, builder, build, new BuilderIntegers() {
                @Override
                public int get(final int index) {
                    return values.get(index);
                }

                @Override
                public int[] toArray() {
                    return values.toArray();
                }

                @Override
                public int size() {
                    return values.size();
                }
            });
        } finally {
            builder.close();
        }
    }

    static interface BuilderIntegers {
        int get(int index);

        int[] toArray();

        int size();
    }

    static IntArrayAtomicFieldData build(final AtomicReader reader, final FieldDataType fieldDataType, final OrdinalsBuilder builder, final Ordinals build,
            final BuilderIntegers values) {
        if(!build.isMultiValued() && CommonSettings.removeOrdsOnSingleValue(fieldDataType)) {
            final Docs ordinals = build.ordinals();
            final FixedBitSet set = builder.buildDocsWithValuesSet();

            // there's sweatspot where due to low unique value count, using ordinals will consume less memory
            final long singleValuesArraySize = reader.maxDoc() * RamUsage.NUM_BYTES_INT
                    + (set == null ? 0 : set.getBits().length * RamUsage.NUM_BYTES_LONG + RamUsage.NUM_BYTES_INT);
            final long uniqueValuesArraySize = values.size() * RamUsage.NUM_BYTES_INT;
            final long ordinalsSize = build.getMemorySizeInBytes();
            if(uniqueValuesArraySize + ordinalsSize < singleValuesArraySize) {
                return new IntArrayAtomicFieldData.WithOrdinals(values.toArray(), reader.maxDoc(), build);
            }

            final int[] sValues = new int[reader.maxDoc()];
            final int maxDoc = reader.maxDoc();
            for(int i = 0; i < maxDoc; i++) {
                sValues[i] = values.get(ordinals.getOrd(i));
            }
            if(set == null) {
                return new IntArrayAtomicFieldData.Single(sValues, reader.maxDoc());
            } else {
                return new IntArrayAtomicFieldData.SingleFixedSet(sValues, reader.maxDoc(), set);
            }
        } else {
            return new IntArrayAtomicFieldData.WithOrdinals(
                    values.toArray(),
                    reader.maxDoc(),
                    build);
        }
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable final Object missingValue, final SortMode sortMode) {
        return new IntValuesComparatorSource(this, missingValue, sortMode);
    }
}
