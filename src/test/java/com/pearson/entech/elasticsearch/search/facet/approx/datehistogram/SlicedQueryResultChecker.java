package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Maps.newHashMap;

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;

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

        protected BucketSpecifier(final String field, final long startTime, final long endTime, final long count) {
            super(field, startTime, endTime, count);
        }

        @Override
        protected int termLimit() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected long getTotalCount(final TermsFacet facet) {
            long count = 0;
            for(final Entry entry : facet.getEntries()) {
                count += entry.getCount();
            }
            return count;
        }

        @Override
        protected void injectAdditionalChecks(final TermsFacet facet) {
            final List<? extends Entry> entries = facet.getEntries();
            final Map<String, Integer> entryCounts = newHashMap();
            for(final Entry entry : entries) {
                entryCounts.put(entry.getTerm().string(), entry.getCount());
            }
            // TODO carry on here
        }
    }

}
