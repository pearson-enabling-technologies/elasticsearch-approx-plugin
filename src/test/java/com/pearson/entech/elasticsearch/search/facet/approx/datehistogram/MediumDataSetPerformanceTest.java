package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    // TODO count facet in value_field mode as well
    @Test
    public void test1000CountFacets() throws Exception {
        final List<RandomDateFacetQuery> randomFacets = nRandomDateFacets(10); // FIXME n=1000
        testSomeRandomFacets(randomFacets);
    }

    // TODO tests for approx counting... will need a preset tolerance
    @Test
    public void test1000ExactDistinctFacets() throws Exception {
        // FIXME decide random field for each query
        final List<RandomDistinctDateFacetQuery> randomFacets = nRandomDistinctFacets(10, randomField(), Integer.MAX_VALUE); // FIXME n=1000
        testSomeRandomFacets(randomFacets);
    }

    // TODO sliced facet in value_field mode as well
    @Test
    public void test1000SlicedFacets() throws Exception {
        // FIXME decide random field for each query
        final List<RandomSlicedDateFacetQuery> randomFacets = nRandomSlicedFacets(1, randomField()); // FIXME n=1000
        testSomeRandomFacets(randomFacets);
    }

    @Test
    //    @Ignore
    public void testBringUpServerForManualQuerying() throws Exception {
        Thread.sleep(10000000);
    }

    private String randomField() {
        return randomPick(_fieldNames);
    }

    private <T extends RandomDateFacetQuery> void testSomeRandomFacets(final List<T> randomFacets) throws Exception {
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

    private List<RandomDateFacetQuery> nRandomDateFacets(final int n) {
        final List<RandomDateFacetQuery> requests = newArrayList();
        for(int i = 0; i < n; i++) {
            requests.add(new RandomDateFacetQuery("RandomDateFacet" + i));
        }
        return requests;
    }

    private List<RandomDistinctDateFacetQuery> nRandomDistinctFacets(final int n, final String distinctField, final int exactThreshold) {
        final List<RandomDistinctDateFacetQuery> requests = newArrayList();
        for(int i = 0; i < n; i++) {
            requests.add(new RandomDistinctDateFacetQuery("RandomDistinctDateFacet" + i, distinctField, exactThreshold));
        }
        return requests;
    }

    private List<RandomSlicedDateFacetQuery> nRandomSlicedFacets(final int n, final String sliceField) {
        final List<RandomSlicedDateFacetQuery> requests = newArrayList();
        for(int i = 0; i < n; i++) {
            requests.add(new RandomSlicedDateFacetQuery("RandomSlicedDateFacet" + i, sliceField));
        }
        return requests;
    }

    private class RandomSlicedDateFacetQuery extends RandomDateFacetQuery {

        private final String _sliceField;

        private RandomSlicedDateFacetQuery(final String facetName, final String sliceField) {
            super(facetName);
            _sliceField = sliceField;
        }

        @Override
        protected DateFacetBuilder makeFacet(final String name) {
            return super.makeFacet(name).sliceField(_sliceField);
        }

        @Override
        public String queryType() {
            return "sliced_date_facet";
        }

        @Override
        protected String facetField() {
            return _sliceField;
        }

        @Override
        public CountingQueryResultChecker buildChecker() {
            return new SlicedQueryResultChecker(_index, _dtField, _sliceField, client());
        }

        // TODO do we need checkHeaders or are they the same as in counting query?

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
            return "distinct_date_facet";
        }

        @Override
        protected String facetField() {
            return _distinctField;
        }

        @Override
        public CountingQueryResultChecker buildChecker() {
            return new DistinctQueryResultChecker(_index, _dtField, client());
        }

        @Override
        protected void checkHeaders(final Facet facet) {
            super.checkHeaders(facet);
            final DistinctQueryResultChecker checker = (DistinctQueryResultChecker) getChecker();
            final HasDistinct castFacet = (HasDistinct) facet;
            checker.checkTotalDistinctCount(castFacet.getDistinctCount());
        }

    }

    public class RandomDateFacetQuery implements Callable<SearchResponse> {

        // TODO add all the other parameters; add range filters too
        // TODO subclasses for the other facet types

        private final String _facetName;
        private final CountingQueryResultChecker _checker;

        private RandomDateFacetQuery(final String facetName) {
            _facetName = facetName;
            _checker = buildChecker();
        }

        public CountingQueryResultChecker buildChecker() {
            return new CountingQueryResultChecker(_index, _dtField, client());
        }

        public CountingQueryResultChecker getChecker() {
            if(_checker == null)
                throw new IllegalStateException("Checker not yet configured, check test logic");
            return _checker;
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

        protected String facetField() {
            return _dtField;
        }

        @Override
        public SearchResponse call() throws Exception {
            return client()
                    .prepareSearch(_index)
                    .setQuery(
                            QueryBuilders.filteredQuery(
                                    QueryBuilders.matchAllQuery(),
                                    makeFilter()))
                    .setSearchType(SearchType.COUNT)
                    .addFacet(makeFacet(_facetName))
                    .execute().actionGet();
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
                _checker.specifier(facetField(), castFacet, i).validate();
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
