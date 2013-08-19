package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;

public class CountingQueryResultChecker {

    private final String _index;
    private final String _dtField;
    private final Client _client;
    private final List<BucketSpecifier> _specs;

    public CountingQueryResultChecker(final String index, final String dtField,
            final Client client) {
        _index = index;
        _dtField = dtField;
        _client = client;
        _specs = newArrayList();
    }

    public BucketSpecifier specifier(final String field, final DateFacet<TimePeriod<NullEntry>> facet, final int idx) {
        final List<? extends TimePeriod<?>> entries = facet.getEntries();
        final long startTime = entries.get(idx).getTime();
        final long endTime = (idx + 1 < entries.size()) ?
                entries.get(idx + 1).getTime() : Long.MAX_VALUE;
        final long count = entries.get(idx).getTotalCount();
        final BucketSpecifier spec = new BucketSpecifier(field, startTime, endTime, count);
        _specs.add(spec);
        return spec;
    }

    public void checkTotalCount(final long totalCount) {
        long expected = 0;
        for(final BucketSpecifier spec : _specs) {
            expected += spec._count;
        }
        assertEquals(expected, totalCount);
    }

    protected String getIndex() {
        return _index;
    }

    protected String getDtField() {
        return _dtField;
    }

    protected Client getClient() {
        return _client;
    }

    protected List<BucketSpecifier> getSpecs() {
        return _specs;
    }

    public class BucketSpecifier {

        private final String _field;
        private final long _startTime;
        private final long _endTime;
        private final long _count;

        private SearchResponse _response;

        protected BucketSpecifier(final String field, final long startTime, final long endTime, final long count) {
            _field = field;
            _startTime = startTime;
            _endTime = endTime;
            _count = count;
        }

        protected String getField() {
            return _field;
        }

        protected long getStartTime() {
            return _startTime;
        }

        protected long getEndTime() {
            return _endTime;
        }

        protected long getCount() {
            return _count;
        }

        protected SearchResponse getResponse() {
            return _response;
        }

        protected int termLimit() {
            return 0;
        }

        public void validate() {
            _response = toSearchRequest().execute().actionGet();
            final TermsFacet facet = _response.getFacets().facet("bucket_check");
            final long totalCount = facet.getTotalCount();
            injectAdditionalChecks(facet);
            assertEquals("Mismatch between total counts for bucket on "
                    + _field, totalCount, _count);
        }

        protected void injectAdditionalChecks(final TermsFacet facet) {
            // no-op
        }

        protected SearchRequestBuilder toSearchRequest() {
            return getClient()
                    .prepareSearch(getIndex())
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    makeFilter()))
                    .setSearchType(SearchType.COUNT)
                    .addFacet(
                            FacetBuilders.termsFacet("bucket_check")
                                    .field(_field)
                                    .size(termLimit()));
        }

        protected FilterBuilder makeFilter() {
            return FilterBuilders.boolFilter()
                    .must(FilterBuilders.rangeFilter(getDtField())
                            .from(_startTime)
                            .to(_endTime)
                            .includeUpper(false));
        }

    }

}
