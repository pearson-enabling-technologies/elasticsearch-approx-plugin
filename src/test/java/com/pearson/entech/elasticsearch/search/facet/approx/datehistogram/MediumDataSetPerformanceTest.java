package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.concurrent.Callable;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilder;
import org.junit.Test;

public class MediumDataSetPerformanceTest extends MediumDataSetTest {

    private final FacetBucketResultChecker _checker =
            new FacetBucketResultChecker(_index, _type, _dtField, client());

    @Test
    public void smokeTest() throws Exception {
        final FacetBucketResultChecker.BucketSpecifier foo = _checker.specifier("", 0, 0, 0);
    }

    private List<Callable<SearchResponse>> nRandomFacets(final int n, final int exactThreshold) {
        final List<Callable<SearchResponse>> requests = newArrayList();
        for(int i = 0; i < n; i++) {
            requests.add(new RandomDateFacetQuery("RandomDateFacet 1", exactThreshold));
        }
        return requests;
    }

    //    private List<BucketSpecifier> toBucketSpecs(final List<SearchResponse> responses) {
    //        final List<BucketSpecifier> specs = newArrayList();
    //        for(final SearchResponse response : responses) {
    //            for(final Facet facet : response.getFacets()) {
    //                // FIXME this API is horrible to use
    //                final DateFacet<TimePeriod<NullEntry>> df = (DateFacet<TimePeriod<NullEntry>>) facet;
    //                for(final TimePeriod<NullEntry> period : df.getTimePeriods()) {
    //                    specs.add(new BucketSpecifier(field, startTime, endTime, bucketCount))
    //                }
    //            }
    //        }
    //
    //    }

    private class RandomDateFacetQuery implements Callable<SearchResponse> {

        // TODO add all the other parameters; add range filters too
        // TODO subclasses for the other facet types

        SearchRequestBuilder request;

        RandomDateFacetQuery(final String name, final int exactThreshold) {
            request = client()
                    .prepareSearch(_index)
                    .setTypes(_type)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSearchType(SearchType.COUNT)
                    .addFacet(makeFacet(name, exactThreshold))
                    .setFilter(makeFilter());
        }

        protected FacetBuilder makeFacet(final String name, final int exactThreshold) {
            return new DateFacetBuilder(name)
                    .exactThreshold(exactThreshold)
                    .interval(randomPick(_intervals))
                    .keyField(_dtField);
        }

        protected FilterBuilder makeFilter() {
            return FilterBuilders.matchAllFilter();
        }

        @Override
        public SearchResponse call() throws Exception {
            return request.execute().actionGet();
        }

    }

}
