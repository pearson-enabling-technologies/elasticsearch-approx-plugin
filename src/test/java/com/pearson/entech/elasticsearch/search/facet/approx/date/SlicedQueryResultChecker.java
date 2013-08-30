package com.pearson.entech.elasticsearch.search.facet.approx.date;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;

import com.pearson.entech.elasticsearch.search.facet.approx.date.DateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.Slice;
import com.pearson.entech.elasticsearch.search.facet.approx.date.TimePeriod;
import com.pearson.entech.elasticsearch.search.facet.approx.date.XContentEnabledList;
import com.pearson.entech.elasticsearch.search.facet.approx.date.MediumDataSetPerformanceTest.RandomSlicedDateFacetQuery;

public class SlicedQueryResultChecker extends CountingQueryResultChecker {

    private final String _sliceField;
    private final RandomSlicedDateFacetQuery _query;

    public SlicedQueryResultChecker(final String index, final String dtField, final String sliceField, final Client client,
            final RandomSlicedDateFacetQuery query) {
        super(index, dtField, client);
        _sliceField = sliceField;
        _query = query;
    }

    @Override
    protected BucketSpecifier buildBucketSpecifier(final String field, final long startTime, final long endTime, final long count) {
        return new BucketSpecifier(field, startTime, endTime, count, _query.facetName());
    }

    public class BucketSpecifier extends CountingQueryResultChecker.BucketSpecifier {

        private final String _origFacetName;

        protected BucketSpecifier(final String field, final long startTime, final long endTime, final long count, final String origFacetName) {
            super(field, startTime, endTime, count);
            _origFacetName = origFacetName;
        }

        @Override
        protected int termLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected BoolFilterBuilder makeFilter() {
            return super.makeFilter()
                    .must(FilterBuilders.existsFilter(getField()));
        }

        @Override
        protected void injectAdditionalChecks(final TermsFacet facet) {
            final DateFacet<TimePeriod<XContentEnabledList<Slice<String>>>> original =
                    _query.getSearchResponse().getFacets().facet(_origFacetName);
            final List<Slice<String>> period = findPeriod(original, getStartTime());

            assertEquals("Number of terms in facet does not match what we'd expect",
                    facet.getEntries().size(), period.size());

            // Get terms from terms list in validation query for this bucket
            final List<? extends Entry> entries = facet.getEntries();
            final Map<String, Integer> entryCounts = newHashMap();
            for(final Entry entry : entries) {
                entryCounts.put(entry.getTerm().string(), entry.getCount());
            }
            // Compare to terms in this period from original facet that we're checking
            for(final Slice<String> slice : period) {
                final String term = slice.getLabel();
                final long expectedCount = entryCounts.get(term).longValue();
                final long actualCount = slice.getTotalCount();
                assertEquals("Counts for term " + term + " don't match what we'd expect",
                        expectedCount, actualCount);
            }
        }

        private List<Slice<String>> findPeriod(final DateFacet<TimePeriod<XContentEnabledList<Slice<String>>>> original, final long startTime) {
            for(final TimePeriod<XContentEnabledList<Slice<String>>> period : original.entries()) {
                if(period.getTime() == startTime)
                    return period.getEntry();
            }
            throw new IllegalArgumentException("Couldn't locate time period starting at " + startTime + " in facet provided");
        }
    }

}
