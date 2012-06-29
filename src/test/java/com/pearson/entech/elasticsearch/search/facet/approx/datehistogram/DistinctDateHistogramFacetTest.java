package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Math.abs;
import static java.lang.Math.random;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacet.Entry;

public class DistinctDateHistogramFacetTest {

    private static Node __node;

    private static final long[] __days = {
            1325376000000L,
            1325376000000L + 86400000,
            1325376000000L + 86400000 * 2,
            1325376000000L + 86400000 * 3,
            1325376000000L + 86400000 * 4,
            1325376000000L + 86400000 * 5,
            1325376000000L + 86400000 * 6,
            1325376000000L + 86400000 * 7
    };

    private static final String __index = "myindex";

    private static final String __type = "testtype";

    private static final String __tsField = "timestamp";

    private static final String __txtField = "txt";

    private static final String __userField = "user";

    private static final String __facetName = "histogram";

    private static final AtomicLong __counter = new AtomicLong(0);

    @BeforeClass
    public static void setUpClass() {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("path.data", "target")
                .build();
        __node = nodeBuilder().local(true).settings(settings).node();
        __node.start();
    }

    @AfterClass
    public static void tearDownClass() {
        __node.close();
    }

    @Before
    public void setUp() throws IOException {
        client().admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
        client().admin().indices().create(new CreateIndexRequest(__index)).actionGet();
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        final String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(__type)
                .startObject("_all").field("enabled", false).endObject()
                .startObject("_source").field("enabled", false).endObject()
                .startObject("properties")
                .startObject(__tsField).field("type", "date").field("store", "yes").endObject()
                .startObject(__txtField).field("type", "string").field("store", "yes").endObject()
                .endObject()
                .endObject()
                .endObject().string();
        client().admin().indices()
                .preparePutMapping(__index)
                .setType(__type)
                .setSource(mapping)
                .execute().actionGet();
        assertEquals(0L, countAll());
    }

    @Test
    public void testWithMaxOneDocPerDayBucketOnAtomicField() throws Exception {
        putSync(newID(), "bart", __days[0]);
        putSync(newID(), "bart", __days[2]);
        putSync(newID(), "bart", __days[4]);
        putSync(newID(), "bart", __days[6]);
        assertEquals(4, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField);
        assertEquals(4, response.hits().getTotalHits());
        final DistinctDateHistogramFacet facet = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList = newArrayList(facet);
        // Expecting just one hit and one distinct hit per doc, for the username.
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(1, facetList.get(0).count());
        assertEquals(1, facetList.get(0).distinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(1, facetList.get(1).count());
        assertEquals(1, facetList.get(1).distinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(1, facetList.get(2).count());
        assertEquals(1, facetList.get(2).distinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(1, facetList.get(3).count());
        assertEquals(1, facetList.get(3).distinctCount());
    }

    @Test
    public void testWithMaxOneDocPerDayBucketOnAnalysedField() throws Exception {
        putSync(newID(), "bart", __days[0]);
        putSync(newID(), "bart", __days[2]);
        putSync(newID(), "bart", __days[4]);
        putSync(newID(), "bart", __days[6]);
        assertEquals(4, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __txtField);
        assertEquals(4, response.hits().getTotalHits());
        final DistinctDateHistogramFacet facet = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList = newArrayList(facet);
        // Expecting one hit for each token in the string "Document created [at] <TIMESTAMP>"
        // for each document, in this case these are unique per bucket too. The word "at"
        // is a stopword and is removed.
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(3, facetList.get(0).count());
        assertEquals(3, facetList.get(0).distinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(3, facetList.get(1).count());
        assertEquals(3, facetList.get(1).distinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(3, facetList.get(2).count());
        assertEquals(3, facetList.get(2).distinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(3, facetList.get(3).count());
        assertEquals(3, facetList.get(3).distinctCount());
    }

    @Test
    public void testWithMultipleDocsPerDayBucketOnAtomicField() throws Exception {
        putSync(newID(), "bart", __days[0]);
        putSync(newID(), "lisa", __days[0] + 10);
        putSync(newID(), "bart", __days[0] + 20);
        putSync(newID(), "bart", __days[2]);
        putSync(newID(), "bart", __days[4]);
        putSync(newID(), "bart", __days[6]);
        putSync(newID(), "homer", __days[6] + 10);
        putSync(newID(), "marge", __days[6] + 20);
        assertEquals(8, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField);
        assertEquals(8, response.hits().getTotalHits());
        final DistinctDateHistogramFacet facet = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList = newArrayList(facet);
        // Hits and distinct hits can now vary in intervals where the same user posted more
        // than once (i.e. day 0 here).
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(3, facetList.get(0).count());
        assertEquals(2, facetList.get(0).distinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(1, facetList.get(1).count());
        assertEquals(1, facetList.get(1).distinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(1, facetList.get(2).count());
        assertEquals(1, facetList.get(2).distinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(3, facetList.get(3).count());
        assertEquals(3, facetList.get(3).distinctCount());
    }

    @Test
    public void testWithMultipleDocsPerDayBucketOnAnalysedField() throws Exception {
        putSync(newID(), "bart", __days[0]);
        putSync(newID(), "lisa", __days[0] + 10);
        putSync(newID(), "bart", __days[0] + 20);
        putSync(newID(), "bart", __days[2]);
        putSync(newID(), "bart", __days[4]);
        putSync(newID(), "bart", __days[6]);
        putSync(newID(), "homer", __days[6] + 10);
        putSync(newID(), "marge", __days[6] + 20);
        assertEquals(8, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __txtField);
        assertEquals(8, response.hits().getTotalHits());
        final DistinctDateHistogramFacet facet = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList = newArrayList(facet);
        // Now things get a bit more complex as all the posts are identically worded apart
        // from the timestamp at the end. 3 tokens indexed per each instance of the field.
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(3 * 3, facetList.get(0).count());
        assertEquals(2 + (1 * 3), facetList.get(0).distinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(1 * 3, facetList.get(1).count());
        assertEquals(1 * 3, facetList.get(1).distinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(1 * 3, facetList.get(2).count());
        assertEquals(1 * 3, facetList.get(2).distinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(3 * 3, facetList.get(3).count());
        assertEquals(2 + (1 * 3), facetList.get(3).distinctCount());
    }

    @Test
    public void testRandomizedWithManyItemsOnDayBucketAboveApproxThreshold() throws Exception {
        final int[] itemsPerDay = prepareRandomData();
        final int totalItems = add(itemsPerDay);
        assertEquals(totalItems, countAll());

        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField);
        final DistinctDateHistogramFacet facet1 = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList1 = newArrayList(facet1);
        assertEquals(7, facetList1.size()); // This is an assumption, I admit.
        for(int i = 0; i < 7; i++) {
            final int exactUsers = itemsPerDay[i];
            assertEquals(exactUsers, facetList1.get(i).count());
            final int tolerance = exactUsers / 100;
            final long fuzzyUsers = facetList1.get(i).distinctCount();
            //System.out.println("Exact user count = " + exactUsers);
            //System.out.println("Fuzzy user count = " + fuzzyUsers);
            assertTrue(abs(fuzzyUsers - exactUsers) < tolerance);
        }

        final SearchResponse response2 = getHistogram(__days[0], __days[7], "day", __txtField);
        final DistinctDateHistogramFacet facet2 = response2.facets().facet(__facetName);
        final ArrayList<Entry> facetList2 = newArrayList(facet2);
        assertEquals(7, facetList2.size()); // This is an assumption, I admit.
        for(int i = 0; i < 7; i++) {
            final int exactTokens = itemsPerDay[i] * 3; // "Document created [by] <ID>"
            final int exactDistinctTokens = itemsPerDay[i] + 2;
            assertEquals(exactTokens, facetList2.get(i).count());
            final int tolerance = exactDistinctTokens / 100;
            final long fuzzyDistinctTokens = facetList2.get(i).distinctCount();
            //System.out.println("Exact distinct token count = " + exactDistinctTokens);
            //System.out.println("Fuzzy distinct token count = " + fuzzyDistinctTokens);
            assertTrue(abs(fuzzyDistinctTokens - exactDistinctTokens) < tolerance);
        }
    }

    @Test
    public void testRandomizedWithManyItemsOnDayBucketBelowApproxThreshold() throws Exception {
        final int[] itemsPerDay = prepareRandomData();
        final int totalItems = add(itemsPerDay);
        assertEquals(totalItems, countAll());

        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField, 1000000);
        final DistinctDateHistogramFacet facet1 = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList1 = newArrayList(facet1);
        assertEquals(7, facetList1.size()); // This is an assumption, I admit.
        for(int i = 0; i < 7; i++) {
            final int exactUsers = itemsPerDay[i];
            assertEquals(exactUsers, facetList1.get(i).count());
            final long retrievedUsers = facetList1.get(i).distinctCount();
            assertEquals(exactUsers, retrievedUsers);
        }

        final SearchResponse response2 = getHistogram(__days[0], __days[7], "day", __txtField, 1000000);
        final DistinctDateHistogramFacet facet2 = response2.facets().facet(__facetName);
        final ArrayList<Entry> facetList2 = newArrayList(facet2);
        assertEquals(7, facetList2.size()); // This is an assumption, I admit.
        for(int i = 0; i < 7; i++) {
            final int exactTokens = itemsPerDay[i] * 3; // "Document created [by] <ID>"
            final int exactDistinctTokens = itemsPerDay[i] + 2;
            assertEquals(exactTokens, facetList2.get(i).count());
            final int tolerance = exactDistinctTokens / 100;
            final long retrievedDistinctTokens = facetList2.get(i).distinctCount();
            assertEquals(exactDistinctTokens, retrievedDistinctTokens);
        }
    }

    // Helper methods

    private static String newID() {
        return String.valueOf(__counter.getAndIncrement());
    }

    private SearchResponse getHistogram(final long start, final long end, final String interval, final String valueField) {
        return getHistogram(start, end, interval, valueField, 0);
    }

    private SearchResponse getHistogram(final long start, final long end, final String interval, final String valueField, final int maxExactPerShard) {
        final FilterBuilder range =
                FilterBuilders.numericRangeFilter(__tsField)
                        .from(start)
                        .to(end);
        final DistinctDateHistogramFacetBuilder facet =
                new DistinctDateHistogramFacetBuilder(__facetName)
                        .keyField(__tsField)
                        .valueField(valueField)
                        .facetFilter(range)
                        .interval(interval)
                        .maxExactPerShard(maxExactPerShard);
        return client().prepareSearch(__index)
                .setSearchType(SearchType.COUNT)
                .addFacet(facet)
                .execute().actionGet();
    }

    private void putSync(final String id, final String user, final long timestamp) throws ElasticSearchException, IOException {
        client().prepareIndex(__index, __type, id)
                .setRefresh(true)
                .setRouting(id)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field(__txtField, "Document created at " + timestamp)
                        .field(__userField, user)
                        .field(__tsField, timestamp)
                        .endObject()).execute().actionGet();
    }

    private void putBulk(final String[] ids, final String[] users, final long[] timestamps) throws Exception {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for(int i = 0; i < ids.length; i++) {
            bulk.add(new IndexRequest(__index, __type, ids[i])
                    .routing(ids[i])
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field(__txtField, "Document created by " + users[i])
                            .field(__userField, users[i])
                            .field(__tsField, timestamps[i])
                            .endObject()));
        }
        bulk.setRefresh(true).execute().actionGet();
    }

    private int[] prepareRandomData() throws Exception {
        final int[] itemsPerDay = new int[7];
        final int minPerDay = 10000;
        final int variationPerDay = 5000;
        for(int i = 0; i < 7; i++) {
            itemsPerDay[i] = minPerDay + (int) (random() * variationPerDay);
            final String[] ids = new String[itemsPerDay[i]];
            final long[] timestamps = new long[itemsPerDay[i]];
            for(int j = 0; j < itemsPerDay[i]; j++) {
                timestamps[j] = __days[i] + (int) (random() * 86400000);
                ids[j] = newID();
            }
            putBulk(ids, ids, timestamps);
        }
        return itemsPerDay;
    }

    private int add(final int[] ints) {
        int total = 0;
        for(final int i : ints) {
            total += i;
        }
        return total;
    }

    private long countAll() {
        return client()
                .prepareCount("_all")
                .execute()
                .actionGet()
                .count();
    }

    private Client client() {
        return __node.client();
    }

}
