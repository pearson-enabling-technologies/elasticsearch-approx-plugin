package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.Facets;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

public class MediumDataSetPerformanceTest extends MediumDataSetTest {

    private static final Map<String, Long> __executionStartTimes = newHashMap();

    private static final Map<String, Long> __executionEndTimes = newHashMap();

    ExecutorService _singleThread = Executors.newSingleThreadExecutor();

    private final boolean _hotThreads = true;

    // TODO we don't have unit tests for sliced distinct facets yet

    // TODO add instrumentation for heap usage
    private void clearMemory() throws Exception {
        client().admin().indices().prepareClearCache(_index).execute().actionGet();
        System.gc();
        Thread.sleep(2000);
    }

    @AfterClass
    public static void tearDownClass() {
        System.out.println("Completed tests:");
        for(final String name : __executionEndTimes.keySet()) {
            final long duration = __executionEndTimes.get(name) - __executionStartTimes.get(name);
            System.out.println(name + " completed in " + duration + " ms");
        }
    }

    // TODO count facet in value_field mode as well
    @Test
    public void test100CountFacets() throws Exception {
        final List<RandomDateFacetQuery> randomFacets = nRandomDateFacets(100); // FIXME
        testSomeRandomFacets(randomFacets, "test100CountFacets");
    }

    @Test
    public void test100ApproxDistinctFacets() throws Exception {
        final List<RandomDistinctDateFacetQuery> randomFacets = nRandomDistinctFacets(100, 0, 0.01);
        testSomeRandomFacets(randomFacets, "test100ApproxDistinctFacets");
    }

    @Test
    public void test100MixedDistinctFacets() throws Exception {
        final List<RandomDistinctDateFacetQuery> randomFacets = nRandomDistinctFacets(100, 5000, 0.01);
        testSomeRandomFacets(randomFacets, "test100MixedDistinctFacets");
    }

    @Test
    public void test100ExactDistinctFacets() throws Exception {
        final List<RandomDistinctDateFacetQuery> randomFacets = nRandomDistinctFacets(100, Integer.MAX_VALUE, 0);
        testSomeRandomFacets(randomFacets, "test100ExactDistinctFacets");
    }

    // TODO sliced facet in value_field mode as well
    @Test
    public void test100SlicedFacets() throws Exception {
        final List<RandomSlicedDateFacetQuery> randomFacets = nRandomSlicedFacets(100);
        testSomeRandomFacets(randomFacets, "test100SlicedFacets");
    }

    @Test
    @Ignore
    public void testBringUpServerForManualQuerying() throws Exception {
        Thread.sleep(1000000);
    }

    private String randomField() {
        return randomPick(_fieldNames);
    }

    private <T extends RandomDateFacetQuery> void testSomeRandomFacets(final List<T> randomFacets, final String testName) throws Exception {
        final List<SearchResponse> responses = executeSerially(randomFacets, testName);
        assertEquals(randomFacets.size(), responses.size());
        for(int i = 0; i < randomFacets.size(); i++) {
            randomFacets.get(i).checkResults(responses.get(i));
        }
    }

    private <T> List<T> executeSerially(final List<? extends Callable<T>> tasks, final String testName) throws Exception {
        clearMemory();
        final ListenableActionFuture<NodesHotThreadsResponse> threads = _hotThreads ?
                new NodesHotThreadsRequestBuilder(client().admin().cluster())
                        .setInterval(TimeValue.timeValueSeconds(2))
                        .setType("cpu").setThreads(4).execute()
                : null;
        logExecutionStart(testName);
        final List<Future<T>> futures = _singleThread.invokeAll(tasks);
        logExecutionEnd(testName);
        if(_hotThreads) {
            final NodeHotThreads[] nodes = threads.actionGet().getNodes();
            System.out.println("Hot threads for " + testName);
            dumpHotThreads(nodes);
        }
        final List<T> results = newArrayList();
        for(final Future<T> future : futures) {
            results.add(future.get());
        }
        return results;
    }

    private void dumpHotThreads(final NodeHotThreads[] nodes) {
        for(final NodeHotThreads node : nodes) {
            System.out.println(node.getHotThreads());
        }
    }

    private List<RandomDateFacetQuery> nRandomDateFacets(final int n) {
        final List<RandomDateFacetQuery> requests = newArrayListWithExpectedSize(n);
        for(int i = 0; i < n; i++) {
            requests.add(new RandomDateFacetQuery("RandomDateFacet" + i));
        }
        return requests;
    }

    private List<RandomDistinctDateFacetQuery> nRandomDistinctFacets(final int n, final int exactThreshold, final double tolerance) {
        final String distinctField = randomField();
        final List<RandomDistinctDateFacetQuery> requests = newArrayListWithExpectedSize(n);
        for(int i = 0; i < n; i++) {
            requests.add(new RandomDistinctDateFacetQuery("RandomDistinctDateFacet" + i, distinctField, exactThreshold, tolerance));
        }
        return requests;
    }

    private List<RandomSlicedDateFacetQuery> nRandomSlicedFacets(final int n) {
        final String sliceField = randomField();
        final List<RandomSlicedDateFacetQuery> requests = newArrayListWithExpectedSize(n);
        for(int i = 0; i < n; i++) {
            requests.add(new RandomSlicedDateFacetQuery("RandomSlicedDateFacet" + i, sliceField));
        }
        return requests;
    }

    protected void logExecutionStart(final String testName) {
        __executionStartTimes.put(testName, System.currentTimeMillis());
    }

    protected void logExecutionEnd(final String testName) {
        __executionEndTimes.put(testName, System.currentTimeMillis());
    }

    public class RandomSlicedDateFacetQuery extends RandomDateFacetQuery {

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
            return new SlicedQueryResultChecker(_index, _dtField, _sliceField, client(), this);
        }

    }

    public class RandomDistinctDateFacetQuery extends RandomDateFacetQuery {

        private final String _distinctField;
        private final int _exactThreshold;
        private final double _tolerance;

        private RandomDistinctDateFacetQuery(final String facetName, final String distinctField, final int exactThreshold, final double tolerance) {
            super(facetName);
            _distinctField = distinctField;
            _exactThreshold = exactThreshold;
            _tolerance = tolerance;
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
            return new DistinctQueryResultChecker(_index, _dtField, client(), _tolerance);
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

        private final String _facetName;
        private CountingQueryResultChecker _checker;
        private SearchResponse _response;

        private RandomDateFacetQuery(final String facetName) {
            _facetName = facetName;
        }

        protected CountingQueryResultChecker buildChecker() {
            return new CountingQueryResultChecker(_index, _dtField, client());
        }

        public CountingQueryResultChecker getChecker() {
            if(_checker == null)
                _checker = buildChecker();
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

        public SearchResponse getSearchResponse() {
            return _response;
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
            _response = myResponse;
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
                getChecker().specifier(facetField(), castFacet, i).validate();
            }
        }

        protected void checkHeaders(final Facet facet) {
            @SuppressWarnings("unchecked")
            final DateFacet<TimePeriod<NullEntry>> castFacet = (DateFacet<TimePeriod<NullEntry>>) facet;
            long totalCount = 0;
            for(int i = 0; i < castFacet.getEntries().size(); i++) {
                totalCount += castFacet.getEntries().get(i).getTotalCount();
            }
            getChecker().checkTotalCount(totalCount);
        }

    }

}
