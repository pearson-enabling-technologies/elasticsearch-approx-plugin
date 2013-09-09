package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.InternalCountingFacet;

/**
 * A Collector for standard (counting) date facets.
 * 
 * @param <V> the field data type of the optional value field (use NullFieldData if you aren't using the value field)
 */
public class CountingCollector<V extends AtomicFieldData<? extends ScriptDocValues>> extends TimestampFirstCollector<V> {

    /**
     * A map from timestamps to counts.
     */
    private TLongIntHashMap _counts;

    /**
     * Create a new collector.
     * 
     * @param keyFieldData the key field (datetime) data
     * @param valueFieldData the value field data
     * @param tzRounding the rounding to apply to datetime values
     */
    public CountingCollector(final LongArrayIndexFieldData keyFieldData,
            final IndexFieldData<V> valueFieldData, final TimeZoneRounding tzRounding) {
        super(keyFieldData, valueFieldData, tzRounding);
        _counts = CacheRecycler.popLongIntMap();
    }

    /**
     * Create a new collector.
     * 
     * @param keyFieldData
     * @param tzRounding
     */
    public CountingCollector(final LongArrayIndexFieldData keyFieldData,
            final TimeZoneRounding tzRounding) {
        super(keyFieldData, tzRounding);
        _counts = CacheRecycler.popLongIntMap();
    }

    @Override
    public void setNextReader(final AtomicReaderContext context) throws IOException {
        super.setNextReader(context);
    }

    @Override
    public void collect(final int doc) throws IOException {
        super.collect(doc);

        if(!hasValueField()) {
            // We are only counting docs
            while(hasNextTimestamp()) {
                final long time = nextTimestamp();
                _counts.adjustOrPutValue(time, 1, 1);
            }
        } else {
            while(hasNextTimestamp()) {
                // We are counting each occurrence of valueField (regardless of its contents)
                if(!hasNextValue())
                    return;

                final long time = nextTimestamp();
                while(hasNextValue()) {
                    nextValue();
                    _counts.adjustOrPutValue(time, 1, 1);
                }
            }
        }
    }

    @Override
    public InternalFacet build(final String facetName) {
        final InternalFacet facet = new InternalCountingFacet(facetName, _counts);
        _counts = null;
        return facet;
    }

}
