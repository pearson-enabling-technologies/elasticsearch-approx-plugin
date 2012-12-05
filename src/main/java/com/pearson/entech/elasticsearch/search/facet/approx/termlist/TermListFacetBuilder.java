package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.AbstractFacetBuilder;

// TODO: Auto-generated Javadoc
/**
 * The Class TermListFacetBuilder.
 */
public class TermListFacetBuilder extends AbstractFacetBuilder {

    /** The key field name. */
    private String keyFieldName;
    
    /** The max per shard. */
    private int maxPerShard;
    
    /** The read from cache. */
    private boolean readFromCache;

    /**
     * Instantiates a new term list facet builder.
     *
     * @param name the name
     */
    protected TermListFacetBuilder(final String name) {
        super(name);
    }

    // TODO implement field, keyField, maxExactPerShard
    // TODO copy scope, facetFilter and nested from DistinctDateHistogramFacetBuilder

    /**
     * Marks the facet to run in a specific scope.
     *
     * @param scope the scope
     * @return the term list facet builder
     */
    @Override
    public TermListFacetBuilder scope(final String scope) {
        super.scope(scope);
        return this;
    }

    /**
     * An additional filter used to further filter down the set of documents the facet will run on.
     *
     * @param filter the filter
     * @return the term list facet builder
     */
    @Override
    public TermListFacetBuilder facetFilter(final FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     *
     * @param nested the nested
     * @return the term list facet builder
     */
    @Override
    public TermListFacetBuilder nested(final String nested) {
        this.nested = nested;
        return this;
    }

    /**
     * The field name to retrieve terms for the TermListFacet
     *
     * @param keyField the key field
     * @return the term list facet builder
     */
    public TermListFacetBuilder keyField(final String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    /**
     * The field name to retrieve terms for the TermListFace
     *
     * @param keyField the key field
     * @return the term list facet builder
     */
    public TermListFacetBuilder field(final String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    /**
     * Max term results per shard.
     *
     * @param maxPerShard the max number of results per shard
     * @return the term list facet builder
     */
    public TermListFacetBuilder maxPerShard(final int maxPerShard) {
        this.maxPerShard = maxPerShard;
        return this;
    }
    
    /**
     * Read from cache.
     *
     * @param readFromCache toggle on/of read from cache 
     * @return the term list facet builder
     */
    public TermListFacetBuilder readFromCache(final boolean readFromCache) {
        this.readFromCache = readFromCache;
        return this;
    }

    /* (non-Javadoc)
     * @see org.elasticsearch.common.xcontent.ToXContent#toXContent(org.elasticsearch.common.xcontent.XContentBuilder, org.elasticsearch.common.xcontent.ToXContent.Params)
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        if(keyFieldName == null) {
            throw new SearchSourceBuilderException("field must be set term list facet for facet [" + name + "]");
        }
        builder.startObject(name); 
        builder.startObject(TermListFacet.TYPE); 
        builder.field("key_field", keyFieldName);

        if(maxPerShard >0) 
            builder.field("max_per_shard", maxPerShard);
        else
            builder.field("max_per_shard", 1000);
        
        builder.field("readFromCache", readFromCache );
        builder.endObject(); 
        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;

    }

}
