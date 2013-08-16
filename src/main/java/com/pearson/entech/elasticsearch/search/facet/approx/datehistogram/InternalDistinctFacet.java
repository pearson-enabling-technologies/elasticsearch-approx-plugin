package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.trove.map.TLongObjectMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacet.ComparatorType;

public class InternalDistinctFacet extends InternalFacet {

    public InternalDistinctFacet(final TLongObjectMap<DistinctCountPayload> _counts, final ComparatorType _comparatorType) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public String getType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BytesReference streamType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        // TODO Auto-generated method stub
        return null;
    }

}
