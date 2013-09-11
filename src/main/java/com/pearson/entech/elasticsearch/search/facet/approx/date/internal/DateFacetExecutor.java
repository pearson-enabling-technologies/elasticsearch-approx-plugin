package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.CountingCollector;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.DistinctCollector;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.NullFieldData;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.SlicedCollector;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.SlicedDistinctCollector;
import com.pearson.entech.elasticsearch.search.facet.approx.date.collectors.TimestampFirstCollector;

/**
 * Executor for all date facets.
 */
public class DateFacetExecutor extends FacetExecutor {

    private final TimestampFirstCollector<?> _collector;

    // TODO proper use of generics

    /**
     * Create a new executor.
     * 
     * @param keyFieldData field data for the datetime field used for timestamps
     * @param valueFieldData field data for the optional value field, can be null
     * @param distinctFieldData field data for the optional distinct field, can be null
     * @param sliceFieldData field data for the optional slice field, can be null
     * @param tzRounding a timezone rounding object
     * @param exactThreshold exact count threshold when doing distincts
     */
    public DateFacetExecutor(final LongArrayIndexFieldData keyFieldData, final IndexFieldData<?> valueFieldData,
            final IndexFieldData<?> distinctFieldData, final IndexFieldData<?> sliceFieldData,
            final TimeZoneRounding tzRounding, final int exactThreshold) {

        if(distinctFieldData == null && sliceFieldData == null)
            if(valueFieldData == null)
                _collector = new CountingCollector<NullFieldData>(keyFieldData, tzRounding);
            else
                _collector = new CountingCollector(keyFieldData, valueFieldData, tzRounding);
        else if(distinctFieldData == null)
            if(valueFieldData == null)
                _collector = new SlicedCollector(keyFieldData, sliceFieldData, tzRounding);
            else
                _collector = new SlicedCollector(keyFieldData, valueFieldData, sliceFieldData, tzRounding);
        else if(sliceFieldData == null)
            if(valueFieldData == null)
                _collector = new DistinctCollector(keyFieldData, distinctFieldData, tzRounding, exactThreshold);
            else
                throw new FacetPhaseExecutionException("unknown date_facet", "Can't use distinct_field and value_field together");
        else if(valueFieldData == null)
            _collector = new SlicedDistinctCollector(keyFieldData, sliceFieldData, distinctFieldData, tzRounding, exactThreshold);
        else
            throw new FacetPhaseExecutionException("unknown date_facet", "Can't use distinct_field and value_field together");
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        return _collector.build(facetName);
    }

    @Override
    public Collector collector() {
        return _collector;
    }

}
