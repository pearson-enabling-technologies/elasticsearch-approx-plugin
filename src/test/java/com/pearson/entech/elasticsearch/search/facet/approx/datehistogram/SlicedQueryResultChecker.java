package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

public class SlicedQueryResultChecker extends CountingQueryResultChecker {

    private final String _sliceField;

    public SlicedQueryResultChecker(final String index, final String dtField, final String sliceField, final Client client) {
        super(index, dtField, client);
        _sliceField = sliceField;
    }

    @Override
    protected BucketSpecifier buildBucketSpecifier(final String field, final long startTime, final long endTime, final long count) {
        return new BucketSpecifier(field, startTime, endTime, count);
    }

    public class BucketSpecifier extends CountingQueryResultChecker.BucketSpecifier {

        // Additional validation rules:
        // Length of "slices" list in original facet entry should equal length of length of "terms" list in validation
        // Count of each term in original facet should equal count of each term in "terms" list

        protected BucketSpecifier(final String field, final long startTime, final long endTime, final long count) {
            super(field, startTime, endTime, count);
        }

        // TODO add "exists" filter to query

        @Override
        protected int termLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected BoolFilterBuilder makeFilter() {
            return super.makeFilter()
                    .must(FilterBuilders.existsFilter(getField()));
        }

        //        @Override
        //        protected void injectAdditionalChecks(final TermsFacet facet) {
        //            final List<? extends Entry> entries = facet.getEntries();
        //            final Map<String, Integer> entryCounts = newHashMap();
        //            for(final Entry entry : entries) {
        //                entryCounts.put(entry.getTerm().string(), entry.getCount());
        //            }
        //            // TODO carry on here
        //        }

    }

}
