package com.pearson.entech.elasticsearch.search.facet.approx.date;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;

import com.pearson.entech.elasticsearch.search.facet.approx.date.DateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.DistinctTimePeriod;
import com.pearson.entech.elasticsearch.search.facet.approx.date.NullEntry;
import com.pearson.entech.elasticsearch.search.facet.approx.date.TimePeriod;

public class DistinctQueryResultChecker extends CountingQueryResultChecker {

    private final double _tolerance;

    public DistinctQueryResultChecker(final String index, final String dtField, final Client client, final double tolerance) {
        super(index, dtField, client);
        _tolerance = tolerance;
    }

    @Override
    public BucketSpecifier specifier(final String field,
            final DateFacet<TimePeriod<NullEntry>> facet, final int idx) {
        final List<? extends TimePeriod<?>> entries = facet.getEntries();
        final DistinctTimePeriod<?> entry = (DistinctTimePeriod<?>) entries.get(idx);
        final long startTime = entry.getTime();
        final long endTime = (idx + 1 < entries.size()) ?
                entries.get(idx + 1).getTime() : Long.MAX_VALUE;
        final long count = entry.getTotalCount();
        final long distinctCount = entry.getDistinctCount();
        final BucketSpecifier spec = new BucketSpecifier(field, startTime, endTime, count, distinctCount);
        getSpecs().add(spec);
        return spec;
    }

    public void checkTotalDistinctCount(final long totalDistinctCount) {
        final Set<String> terms = newHashSet();
        for(final Object o : getSpecs()) {
            final BucketSpecifier spec = (BucketSpecifier) o;
            final TermsFacet facet = spec.getResponse().getFacets().facet("bucket_check");
            for(final Entry entry : facet.getEntries())
                terms.add(entry.getTerm().string());
        }
        final double expectedSize = terms.size();
        final double tolerance = expectedSize * _tolerance;
        assertEquals(terms.size(), totalDistinctCount, tolerance);
    }

    public class BucketSpecifier extends CountingQueryResultChecker.BucketSpecifier {

        private final long _distinctCount;

        protected BucketSpecifier(final String field, final long startTime, final long endTime, final long count, final long distinctCount) {
            super(field, startTime, endTime, count);
            _distinctCount = distinctCount;
        }

        @Override
        protected void injectAdditionalChecks(final TermsFacet facet) {
            final int facetSize = facet.getEntries().size();
            final double tolerance = facetSize * _tolerance;
            assertEquals("Distinct count not equal to number of terms received by terms facet on field " + getField(),
                    facetSize, _distinctCount, tolerance);
        }

        @Override
        protected int termLimit() {
            return Integer.MAX_VALUE;
        }

    }

}
