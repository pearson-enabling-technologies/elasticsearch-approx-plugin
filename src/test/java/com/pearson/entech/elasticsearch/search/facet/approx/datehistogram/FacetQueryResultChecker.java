package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.MediumDataSetPerformanceTest.RandomDateFacetQuery;

public class FacetQueryResultChecker {

    private final ExecutorService _singleThread = Executors.newSingleThreadExecutor();

    private final List<Callable<Boolean>> _checkers;

    public FacetQueryResultChecker(final RandomDateFacetQuery query, final SearchResponse response, final Client client) {
        _checkers = buildEntryCheckers(query, response, client);
        _checkers.add(buildHeaderChecker(query, response, client));
    }

    public void check() throws Exception {
        for(final Callable<Boolean> checker : _checkers) {
            checker.call();
        }
    }

    private List<Callable<Boolean>> buildEntryCheckers(final RandomDateFacetQuery query, final SearchResponse response, final Client client) {
        final List<Callable<Boolean>> checkers = newArrayList();
        final DateFacet<TimePeriod<NullEntry>> facet = response.getFacets().facet(query.facetName());
        final List<? extends TimePeriod<?>> entries = facet.getEntries();
        for(int i = 0; i < entries.size(); i++) {
            final int idx = i;
            checkers.add(new Callable<Boolean>() {
                @Override
                public State call() throws Exception {
                    final long startTime = entries.get(idx).getTime();
                    final long endTime = (idx + 1 < entries.size()) ?
                            entries.get(idx + 1).getTime() : Long.MAX_VALUE;
                    final long count = entries.get(idx).getTotalCount();
                    new BucketSpecifier(field, startTime, endTime, count).validate();
                    
                    return null;
                }
            }
        );

        final long startTime = entries.get(idx).getTime();
        final long endTime = (idx + 1 < entries.size()) ?
                entries.get(idx + 1).getTime() : Long.MAX_VALUE;
        final long count = entries.get(idx).getTotalCount();
        return new BucketSpecifier(field, startTime, endTime, count);
    }

    private Callable<Boolean> buildHeaderChecker(final RandomDateFacetQuery query, final SearchResponse response, final Client client) {
        // TODO Auto-generated method stub
        return null;
    }

    //    
    //    
    //    
    //    
    //    
    //    
    //    
    //    
    //    
    //    
    //    

    public BucketSpecifier specifier(final String field, final DateFacet<TimePeriod<NullEntry>> facet, final int idx) {
        final List<? extends TimePeriod<?>> entries = facet.getEntries();
        final long startTime = entries.get(idx).getTime();
        final long endTime = (idx + 1 < entries.size()) ?
                entries.get(idx + 1).getTime() : Long.MAX_VALUE;
        final long count = entries.get(idx).getTotalCount();
        return new BucketSpecifier(field, startTime, endTime, count);
    }

    public class BucketSpecifier {

        private final String _field;
        private final long _startTime;
        private final long _endTime;
        private final long _count;

        protected boolean _allTerms = false;

        private BucketSpecifier(final String field, final long startTime, final long endTime, final long count) {
            _field = field;
            _startTime = startTime;
            _endTime = endTime;
            _count = count;
        }

        public void validate() {
            final SearchResponse response = toSearchRequest().execute().actionGet();
            final TermsFacet facet = response.getFacets().facet("bucket_check");
            final long totalCount = facet.getTotalCount();
            assertEquals("Mismatch between total counts for bucket on "
                    + _field, totalCount, _count);
        }

        protected SearchRequestBuilder toSearchRequest() {
            return _client
                    .prepareSearch(_index)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    makeFilter()))
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
