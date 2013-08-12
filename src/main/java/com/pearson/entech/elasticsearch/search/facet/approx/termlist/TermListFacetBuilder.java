package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.FacetBuilder;

public class TermListFacetBuilder extends FacetBuilder {
    private String _fieldName;
    private int _maxPerShard = 100;

    public TermListFacetBuilder(final String name) {
        super(name);

    }

    public TermListFacetBuilder readFromCache(final boolean val) {
        return this;
    }

    public TermListFacetBuilder keyField(final String field) {
        this._fieldName = field;
        return this;
    }

    public TermListFacetBuilder maxPerShard(final int maxPerShard) {
        this._maxPerShard = maxPerShard;
        return this;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(name);
        builder.startObject(TermListFacet.TYPE);
        builder.field("field", _fieldName);
        builder.field("maxPerShard", _maxPerShard);
        //TODO add exclude?
        builder.endObject();
        addFilterFacetAndGlobal(builder, params);
        builder.endObject();
        return builder;
    }

}
