package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.Facets;
import org.junit.Before;
import org.junit.Test;

public class MediumDataSetPerformanceTest extends MediumDataSetTest {

    ExecutorService _singleThread = Executors.newSingleThreadExecutor();

    // TODO add instrumentation for time and heap usage
    @Before
    public void setUp() throws Exception {
        client().admin().indices().prepareClearCache(_index).execute().actionGet();
        System.gc();
    }

    @Test
    public void test1000CountFacets() throws Exception {
        testSomeRandomDateFacets(1000);
    }

    @Test
    public void testBringUpServerForManualQuerying() throws Exception {
        Thread.sleep(10000000);
    }

    private void testSomeRandomDateFacets(final int n) throws Exception {
        final List<RandomDateFacetQuery> randomFacets = nRandomDateFacets(n, Integer.MAX_VALUE);
        testSomeRandomFacets(randomFacets);
    }

    private void testSomeRandomFacets(final List<? extends RandomDateFacetQuery> randomFacets) throws Exception {
        final List<SearchResponse> responses = executeSerially(randomFacets);
        assertEquals(randomFacets.size(), responses.size());
        for(int i = 0; i < randomFacets.size(); i++) {
            randomFacets.get(i).checkResults(responses.get(i));
        }
    }

    private <T> List<T> executeSerially(final List<? extends Callable<T>> tasks) throws Exception {
        final List<Future<T>> futures = _singleThread.invokeAll(tasks);
        final List<T> results = newArrayList();
        for(final Future<T> future : futures) {
            results.add(future.get());
        }
        return results;
    }

    private List<RandomDateFacetQuery> nRandomDateFacets(final int n, final int exactThreshold) {
        final List<RandomDateFacetQuery> requests = newArrayList();
        for(int i = 0; i < n; i++) {
            requests.add(new RandomDateFacetQuery("RandomDateFacet" + i));
        }
        return requests;
    }

    private class RandomDistinctDateFacetQuery extends RandomDateFacetQuery {

        private final String _distinctField;
        private final int _exactThreshold;

        private RandomDistinctDateFacetQuery(final String facetName, final String distinctField, final int exactThreshold) {
            super(facetName);
            _distinctField = distinctField;
            _exactThreshold = exactThreshold;
        }

        @Override
        protected DateFacetBuilder makeFacet(final String name) {
            return super.makeFacet(name).distinctField(_distinctField).exactThreshold(_exactThreshold);
        }

        @Override
        public String queryType() {
            return "disinct_date_facet";
        }

    }

    public class RandomDateFacetQuery implements Callable<SearchResponse> {

        // TODO add all the other parameters; add range filters too
        // TODO subclasses for the other facet types

        protected FacetQueryResultChecker buildChecker() {
            return new FacetQueryResultChecker(_index, _dtField, client());
        }

        private final SearchRequestBuilder _request;
        private final String _facetName;
        private final FacetQueryResultChecker _checker;

        private RandomDateFacetQuery(final String facetName) {
            _facetName = facetName;
            _request = client()
                    .prepareSearch(_index)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    makeFilter()))
                    .setSearchType(SearchType.COUNT)
                    .addFacet(makeFacet(facetName));
            _checker = buildChecker();
        }

        protected DateFacetBuilder makeFacet(final String name) {
            return new DateFacetBuilder(name)
                    .interval(randomPick(_intervals))
                    .keyField(_dtField);
        }

        protected FilterBuilder makeFilter() {
            return FilterBuilders.matchAllFilter();
        }

        protected String queryType() {
            return "counting_date_facet";
        }

        protected String facetName() {
            return _facetName;
        }

        @Override
        public SearchResponse call() throws Exception {
            return _request.execute().actionGet();
        }

        // Validation stuff

        public void checkResults(final SearchResponse myResponse) {
            final Facets facets = myResponse.getFacets();
            assertEquals("Found " + facets.facets().size() + " facets instead of 1", 1, facets.facets().size());
            final Facet facet = facets.facet(_facetName);
            assertEquals(queryType(), facet.getType());
            checkEntries(facet);
            checkHeaders(facet);
        }

        protected void checkEntries(final Facet facet) {
            @SuppressWarnings("unchecked")
            final DateFacet<TimePeriod<NullEntry>> castFacet = (DateFacet<TimePeriod<NullEntry>>) facet;
            for(int i = 0; i < castFacet.getEntries().size(); i++) {
                _checker.specifier(_dtField, castFacet, i).validate();
            }
        }

        protected void checkHeaders(final Facet facet) {
            @SuppressWarnings("unchecked")
            final DateFacet<TimePeriod<NullEntry>> castFacet = (DateFacet<TimePeriod<NullEntry>>) facet;
            long totalCount = 0;
            for(int i = 0; i < castFacet.getEntries().size(); i++) {
                totalCount += castFacet.getEntries().get(i).getTotalCount();
            }
            _checker.checkTotalCount(totalCount);
        }

    }

}
