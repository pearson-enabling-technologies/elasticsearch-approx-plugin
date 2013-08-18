package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static org.junit.Assert.assertEquals;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;

public class FacetBucketResultChecker {

    private final String _index;
    private final String _type;
    private final String _dtField;
    private final Client _client;

    public FacetBucketResultChecker(final String index, final String type,
            final String dtField, final Client client) {
        _index = index;
        _type = type;
        _dtField = dtField;
        _client = client;
    }

    public BucketSpecifier specifier(final String field, final long startTime, final long endTime, final long bucketCount) {
        return new BucketSpecifier(field, startTime, endTime, bucketCount);
    }

    public class BucketSpecifier {

        private final String _field;
        private final long _startTime;
        private final long _endTime;
        private final long _bucketCount;

        protected boolean _allTerms = false;

        public BucketSpecifier(final String field, final long startTime, final long endTime, final long bucketCount) {
            _field = field;
            _startTime = startTime;
            _endTime = endTime;
            _bucketCount = bucketCount;
        }

        public void validate() {
            final SearchResponse response = toSearchRequest().execute().actionGet();
            final TermsFacet facet = response.getFacets().facet("bucket_check");
            final long totalCount = facet.getTotalCount();
            assertEquals("Mismatch between total counts for bucket "
                    + this
                    + ", test query said "
                    + _bucketCount
                    + " but result checker said "
                    + totalCount,
                    totalCount, _bucketCount);
        }

        protected SearchRequestBuilder toSearchRequest() {
            return _client
                    .prepareSearch(_index)
                    .setTypes(_type)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSearchType(SearchType.COUNT)
                    .addFacet(
                            FacetBuilders.termsFacet("bucket_check")
                                    .field(_field)
                                    .allTerms(_allTerms)
                    )
                    .setFilter(makeFilter());
        }

        protected FilterBuilder makeFilter() {
            return FilterBuilders.boolFilter()
                    .must(FilterBuilders.rangeFilter(_dtField)
                            .from(_startTime)
                            .to(_endTime)
                            .includeUpper(false));
        }

    }

}
