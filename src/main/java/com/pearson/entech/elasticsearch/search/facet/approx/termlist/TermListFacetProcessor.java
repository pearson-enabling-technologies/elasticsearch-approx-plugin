package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

public class TermListFacetProcessor extends AbstractComponent implements FacetProcessor {

    /**
     * Instantiates a new term list facet processor.
     *
     * @param settings the settings
     */
    @Inject
    public TermListFacetProcessor(final Settings settings) {
        super(settings);
        InternalTermListFacet.registerStreams();
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.FacetProcessor#parse(java.lang.String, org.elasticsearch.common.xcontent.XContentParser, org.elasticsearch.search.internal.SearchContext)
     */
    @Override
    public FacetCollector parse(final String facetName, final XContentParser parser, final SearchContext context) throws IOException {
        String keyField = null;
        XContentParser.Token token;
        String fieldName = null;
        int maxPerShard = 100;
        boolean readFromCache = false;
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
               else if("read_from_cache".equals(fieldName) || "readFromCache".equals(fieldName)) {
                   readFromCache = parser.booleanValue();
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

        return new TermListFacetCollector(facetName, keyField, context, maxPerShard, readFromCache);
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.FacetProcessor#reduce(java.lang.String, java.util.List)
     */
    @Override
    public Facet reduce(final String name, final List<Facet> facets) { 
        final InternalTermListFacet base = (InternalTermListFacet) facets.get(0);
        return base.reduce(name, facets); 
       
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.search.facet.FacetProcessor#types()
     */
    @Override
    public String[] types() {
        return new String[] { TermListFacet.TYPE, "term_list_facet" };
    }

}
