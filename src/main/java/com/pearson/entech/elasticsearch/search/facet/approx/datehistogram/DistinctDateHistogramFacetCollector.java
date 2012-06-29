package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldData.StringValueInDocProc;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData.LongValueInDocProc;
import org.elasticsearch.index.field.data.longs.LongFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

/**
 * A date histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class DistinctDateHistogramFacetCollector extends AbstractFacetCollector {

    private final String keyFieldName;

    private final DistinctDateHistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType keyFieldType;

    private LongFieldData keyFieldData;

    private FieldData valueFieldData;

    private final KeyFieldVisitor histoProc;

    private final ExtTLongObjectHashMap<DistinctCountPayload> payloads;

    private final String valueFieldName;

    private final FieldDataType valueFieldType;

    private int docBase;

    private final int maxExactPerShard;

    public DistinctDateHistogramFacetCollector(final String facetName, final String keyField, final String valueField, final TimeZoneRounding tzRounding,
            final DistinctDateHistogramFacet.ComparatorType comparatorType, final SearchContext context, final int maxExactPerShard) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();

        this.maxExactPerShard = maxExactPerShard;

        final MapperService.SmartNameFieldMappers keyMappers = context.smartFieldMappers(keyField);
        if(keyMappers == null || !keyMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for key field [" + keyField + "]");
        }

        final MapperService.SmartNameFieldMappers valueMappers = context.smartFieldMappers(valueField);
        if(valueMappers == null || !valueMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for value field [" + valueField + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if(keyMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(keyMappers.docMapper().typeFilter()));
        }

        this.payloads = new ExtTLongObjectHashMap<DistinctCountPayload>();

        final FieldMapper keyMapper = keyMappers.mapper();
        final FieldMapper valueMapper = valueMappers.mapper();

        keyFieldName = keyMapper.names().indexName();
        keyFieldType = keyMapper.fieldDataType();
        valueFieldName = valueMapper.names().indexName();
        valueFieldType = valueMapper.fieldDataType();
        histoProc = new KeyFieldVisitor(tzRounding);
    }

    @Override
    protected void doCollect(final int doc) throws IOException {
        keyFieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override
    protected void doSetNextReader(final IndexReader reader, final int docBase) throws IOException {
        keyFieldData = (LongFieldData) fieldDataCache.cache(keyFieldType, reader, keyFieldName);
        valueFieldData = fieldDataCache.cache(valueFieldType, reader, valueFieldName);
        this.docBase = docBase;
    }

    @Override
    public Facet facet() {
        return new InternalDistinctDateHistogramFacet(facetName, comparatorType, histoProc.counts(), true);
    }

    private void addItem(final long timestamp, final Object item) {
        if(payloads.containsKey(timestamp))
            payloads.get(timestamp).update(item);
        else
            payloads.put(timestamp, new DistinctCountPayload(this.maxExactPerShard).update(item));
    }

    public class KeyFieldVisitor implements LongValueInDocProc {

        private final TimeZoneRounding tzRounding;

        private long currTimestamp;

        public KeyFieldVisitor(final TimeZoneRounding tzRounding) {
            this.tzRounding = tzRounding;
        }

        @Override
        public void onValue(final int docId, final long timestamp) {
            currTimestamp = tzRounding.calc(timestamp);
            valueFieldData.forEachValueInDoc(docId, new ValueFieldVisitor());
        }

        public ExtTLongObjectHashMap<DistinctCountPayload> counts() {
            return payloads;
        }

        public class ValueFieldVisitor implements StringValueInDocProc {

            @Override
            public void onValue(final int docId, final String value) {
                addItem(currTimestamp, value);
            }

            @Override
            public void onMissing(final int docId) {
                // Do nothing
            }

        }

    }

}