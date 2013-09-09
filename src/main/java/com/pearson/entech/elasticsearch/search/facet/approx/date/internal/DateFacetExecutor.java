package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.LongValues.Iter;
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

public class DateFacetExecutor extends FacetExecutor {

    private static final Iter __emptyIter = new Iter.Empty();

    private final LongArrayIndexFieldData _keyFieldData;
    private final IndexFieldData _valueFieldData;
    private final IndexFieldData _distinctFieldData;
    private final IndexFieldData _sliceFieldData;

    private final TimestampFirstCollector _collector;

    private final TimeZoneRounding _tzRounding;

    private final int _exactThreshold;

    public DateFacetExecutor(final LongArrayIndexFieldData keyFieldData, final IndexFieldData valueFieldData,
            final IndexFieldData distinctFieldData, final IndexFieldData sliceFieldData,
            final TimeZoneRounding tzRounding, final int exactThreshold, final boolean debug) {
        _keyFieldData = keyFieldData;
        _valueFieldData = valueFieldData;
        _distinctFieldData = distinctFieldData;
        _sliceFieldData = sliceFieldData;
        _tzRounding = tzRounding;
        _exactThreshold = exactThreshold;

        // TODO type safety for the following constructors

        if(_distinctFieldData == null && _sliceFieldData == null)
            if(_valueFieldData == null)
                _collector = new CountingCollector<NullFieldData>(keyFieldData, tzRounding);
            else
                _collector = new CountingCollector(keyFieldData, _valueFieldData, tzRounding);
        else if(_distinctFieldData == null)
            if(_valueFieldData == null)
                _collector = new SlicedCollector(keyFieldData, sliceFieldData, tzRounding);
            else
                _collector = new SlicedCollector(keyFieldData, valueFieldData, sliceFieldData, tzRounding);
        else if(_sliceFieldData == null)
            if(_valueFieldData == null)
                _collector = new DistinctCollector(keyFieldData, distinctFieldData, tzRounding, exactThreshold);
            else
                throw new FacetPhaseExecutionException("unknown date_facet", "Can't use distinct_field and value_field together");
        else if(_valueFieldData == null)
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
