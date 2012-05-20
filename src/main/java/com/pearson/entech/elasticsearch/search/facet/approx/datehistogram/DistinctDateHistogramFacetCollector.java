package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.map.hash.TLongLongHashMap;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
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

    private final String indexFieldName;

    private final DateHistogramFacet.ComparatorType comparatorType;

    private final FieldDataCache fieldDataCache;

    private final FieldDataType fieldDataType;

    private LongFieldData fieldData;

    private final DateHistogramProc histoProc;

    public DistinctDateHistogramFacetCollector(final String facetName, final String fieldName, final TimeZoneRounding tzRounding,
            final DateHistogramFacet.ComparatorType comparatorType, final SearchContext context) {
        super(facetName);
        this.comparatorType = comparatorType;
        this.fieldDataCache = context.fieldDataCache();

        final MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
        if(smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if(smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        final FieldMapper mapper = smartMappers.mapper();

        indexFieldName = mapper.names().indexName();
        fieldDataType = mapper.fieldDataType();
        histoProc = new DateHistogramProc(tzRounding);
    }

    @Override
    protected void doCollect(final int doc) throws IOException {
        fieldData.forEachValueInDoc(doc, histoProc);
    }

    @Override
    protected void doSetNextReader(final IndexReader reader, final int docBase) throws IOException {
        fieldData = (LongFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
    }

    @Override
    public Facet facet() {
        return new InternalDistinctDateHistogramFacet(facetName, comparatorType, histoProc.counts(), true);
    }

    public static class DateHistogramProc implements LongFieldData.LongValueInDocProc {

        private final TLongLongHashMap counts = CacheRecycler.popLongLongMap();

        private final TimeZoneRounding tzRounding;

        public DateHistogramProc(final TimeZoneRounding tzRounding) {
            this.tzRounding = tzRounding;
        }

        @Override
        public void onValue(final int docId, final long value) {
            counts.adjustOrPutValue(tzRounding.calc(value), 1, 1);
        }

        public TLongLongHashMap counts() {
            return counts;
        }
    }
}