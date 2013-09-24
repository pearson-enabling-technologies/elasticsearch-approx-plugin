package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.FacetBuilder;

public class TermListFacetBuilder extends FacetBuilder {

    private String _fieldName;
    private int _maxPerShard = Constants.DEFAULT_MAX_PER_SHARD;
    private float _sample = Constants.DEFAULT_SAMPLE;

    public TermListFacetBuilder(final String name) {
        super(name);
    }

    public TermListFacetBuilder keyField(final String field) {
        _fieldName = field;
        return this;
    }

    public TermListFacetBuilder maxPerShard(final int maxPerShard) {
        _maxPerShard = maxPerShard;
        return this;
    }

    public TermListFacetBuilder sample(final float sample) {
        _sample = sample;
        return this;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(name);
        builder.startObject(TermListFacet.TYPE);
        builder.field("field", _fieldName);
        builder.field("maxPerShard", _maxPerShard);

        builder.field("sample", _sample);
        builder.endObject();
        addFilterFacetAndGlobal(builder, params);
        builder.endObject();
        return builder;
    }

}
