package com.pearson.entech.elasticsearch.plugin.approx.termlist;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

public class TermListFacetProcessor extends AbstractComponent implements FacetProcessor {

    public TermListFacetProcessor(final Settings settings) {
        super(settings);
        InternalTermListFacet.registerStreams();
    }

    @Override
    public FacetCollector parse(final String facetName, final XContentParser parser, final SearchContext context) throws IOException {
        String keyField = null;
        XContentParser.Token token;
        String fieldName = null;
        int maxPerShard = 1000;
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if(token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if(token.isValue()) {
                if("field".equals(fieldName)) {
                    keyField = parser.text();
                } else if("key_field".equals(fieldName) || "keyField".equals(fieldName)) {
                    keyField = parser.text();
                } else if("max_per_shard".equals(fieldName) || "maxPerShard".equals(fieldName)) {
                    maxPerShard = parser.intValue();
                }
            }
        }

        if(keyField == null) {
            throw new FacetPhaseExecutionException(facetName, "key field is required to be set for term list facet, either using [field] or using [key_field]");
        }

        final FieldMapper mapper = context.smartNameFieldMapper(keyField);
        if(mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "(key) field [" + keyField + "] not found");
        }

        return new TermListFacetCollector(facetName, keyField, context, maxPerShard);
    }

    @Override
    public Facet reduce(final String name, final List<Facet> facets) {
        if(facets.size() == 1) {
            return facets.get(0);
        }

        final TermListFacet base = (TermListFacet) facets.get(0);
        for(int i = 1; i < facets.size(); i++) {
            final TermListFacet other = (TermListFacet) facets.get(i);

        }
    }

    @Override
    public String[] types() {
        // TODO Auto-generated method stub
        return null;
    }

}
