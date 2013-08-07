package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.pow;
import static java.lang.Math.random;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
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

import com.pearson.entech.elasticsearch.facet.approx.datehistogram.DistinctDateHistogramFacetBuilder;
import com.pearson.entech.elasticsearch.facet.approx.datehistogram.InternalDistinctDateHistogramFacet;
import com.pearson.entech.elasticsearch.facet.approx.datehistogram.InternalDistinctDateHistogramFacet.DistinctEntry;

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

    private static final AtomicInteger __counter = new AtomicInteger(0);

    private final Random _random = new Random(0);

    @BeforeClass
    public static void setUpClass() {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                // Reluctantly removed this to reduce overall memory:
                //.put("index.store.type", "memory")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.cache.field.type", "soft")
                .put("index.merge.policy.merge_factor", 30)
                .put("path.data", "target")
                .put("refresh_interval", -1)
                .build();
        __node = nodeBuilder()
                .local(true)
                .settings(settings)
                .clusterName("DistinctDateHistogramFacetTest")
                .node();
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
                .startObject(__tsField).field("type", "date").field("store", "no").endObject()
                .startObject(__txtField).field("type", "string").field("store", "no").endObject()
                .startObject(__userField).field("type", "integer").field("store", "no").endObject()
                .endObject()
                .endObject()
                .endObject().string();
        client().admin().indices()
                .preparePutMapping(__index)
                .setType(__type)
                .setSource(mapping)
                .execute().actionGet();
        client().admin().indices().clearCache(
                new ClearIndicesCacheRequest("_all"));
        System.gc();
        assertEquals(0L, countAll());
    }

    @Test
    public void testWithMaxOneDocPerDayBucketOnAtomicField() throws Exception {
        putSync(newID(), 1, __days[0]);
        putSync(newID(), 1, __days[2]);
        putSync(newID(), 1, __days[4]);
        putSync(newID(), 1, __days[6]);
        assertEquals(4, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField);
        System.out.println(response);
        assertEquals(4, response.getHits().getTotalHits());
        final InternalDistinctDateHistogramFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctEntry> facetList = facet.entries();
        // Expecting just one hit and one distinct hit per doc, for the username.
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(1, facetList.get(0).getTotalCount());
        assertEquals(1, facetList.get(0).getDistinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(1, facetList.get(1).getTotalCount());
        assertEquals(1, facetList.get(1).getDistinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(1, facetList.get(2).getTotalCount());
        assertEquals(1, facetList.get(2).getDistinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(1, facetList.get(3).getTotalCount());
        assertEquals(1, facetList.get(3).getDistinctCount());
    }

    @Test
    public void testWithMaxOneDocPerDayBucketOnAnalysedField() throws Exception {
        putSync(newID(), 1, __days[0]);
        putSync(newID(), 1, __days[2]);
        putSync(newID(), 1, __days[4]);
        putSync(newID(), 1, __days[6]);
        assertEquals(4, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __txtField);
        assertEquals(4, response.getHits().getTotalHits());
        final InternalDistinctDateHistogramFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctEntry> facetList = facet.entries();
        // Expecting one hit for each token in the string "Document created [at] <TIMESTAMP>"
        // for each document, in this case these are unique per bucket too. The word "at"
        // is a stopword and is removed.
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(3, facetList.get(0).getTotalCount());
        assertEquals(3, facetList.get(0).getDistinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(3, facetList.get(1).getTotalCount());
        assertEquals(3, facetList.get(1).getDistinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(3, facetList.get(2).getTotalCount());
        assertEquals(3, facetList.get(2).getDistinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(3, facetList.get(3).getTotalCount());
        assertEquals(3, facetList.get(3).getDistinctCount());
    }

    @Test
    public void testWithMultipleDocsPerDayBucketOnAtomicField() throws Exception {
        putSync(newID(), 1, __days[0]);
        putSync(newID(), 2, __days[0] + 10);
        putSync(newID(), 1, __days[0] + 20);
        putSync(newID(), 1, __days[2]);
        putSync(newID(), 1, __days[4]);
        putSync(newID(), 1, __days[6]);
        putSync(newID(), 3, __days[6] + 10);
        putSync(newID(), 4, __days[6] + 20);
        assertEquals(8, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField);
        assertEquals(8, response.getHits().getTotalHits());
        final InternalDistinctDateHistogramFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctEntry> facetList = facet.entries();
        // Hits and distinct hits can now vary in intervals where the same user posted more
        // than once (i.e. day 0 here).
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(3, facetList.get(0).getTotalCount());
        assertEquals(2, facetList.get(0).getDistinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(1, facetList.get(1).getTotalCount());
        assertEquals(1, facetList.get(1).getDistinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(1, facetList.get(2).getTotalCount());
        assertEquals(1, facetList.get(2).getDistinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(3, facetList.get(3).getTotalCount());
        assertEquals(3, facetList.get(3).getDistinctCount());
    }

    @Test
    public void testWithMultipleDocsPerDayBucketOnAnalysedField() throws Exception {
        putSync(newID(), 1, __days[0]);
        putSync(newID(), 2, __days[0] + 10);
        putSync(newID(), 1, __days[0] + 20);
        putSync(newID(), 1, __days[2]);
        putSync(newID(), 1, __days[4]);
        putSync(newID(), 1, __days[6]);
        putSync(newID(), 3, __days[6] + 10);
        putSync(newID(), 4, __days[6] + 20);
        assertEquals(8, countAll());
        final SearchResponse response = getHistogram(__days[0], __days[7], "day", __txtField);
        assertEquals(8, response.getHits().getTotalHits());
        final InternalDistinctDateHistogramFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctEntry> facetList = facet.entries();
        // Now things get a bit more complex as all the posts are identically worded apart
        // from the timestamp at the end. 3 tokens indexed per each instance of the field.
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(3 * 3, facetList.get(0).getTotalCount());
        assertEquals(2 + (1 * 3), facetList.get(0).getDistinctCount());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(1 * 3, facetList.get(1).getTotalCount());
        assertEquals(1 * 3, facetList.get(1).getDistinctCount());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(1 * 3, facetList.get(2).getTotalCount());
        assertEquals(1 * 3, facetList.get(2).getDistinctCount());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(3 * 3, facetList.get(3).getTotalCount());
        assertEquals(2 + (1 * 3), facetList.get(3).getDistinctCount());
    }

    @Test
    public void testRandomizedWithManyItemsOnDayBucket() throws Exception {
        // Do this 20 times for different amounts of data
        for(int t = 1; t <= 20; t++) {
            setUp();
            final int minPerDay = (int) pow(2, t);
            System.out.println("Randomized testing: inserting minimum " + 7 * minPerDay + " items");
            final int[] itemsPerDay = prepareRandomData(minPerDay);
            final int totalItems = add(itemsPerDay);
            assertEquals(totalItems, countAll());

            System.out.println("Randomized testing: running facet");
            final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField, 1000);
            final InternalDistinctDateHistogramFacet facet1 = response.getFacets().facet(__facetName);
            final List<DistinctEntry> facetList1 = facet1.entries();
            assertEquals(7, facetList1.size());
            for(int i = 0; i < 7; i++) {
                final int exactUsers = itemsPerDay[i];
                assertEquals(exactUsers, facetList1.get(i).getTotalCount());
                final int tolerance = exactUsers / 100;
                final long fuzzyUsers = facetList1.get(i).getDistinctCount();
                //System.out.println("Exact user count = " + exactUsers);
                //System.out.println("Fuzzy user count = " + fuzzyUsers);
                assertTrue(String.format(
                        "With > %d terms per day: Estimated count %d is not within 1%% tolerance of %d",
                        minPerDay, fuzzyUsers, exactUsers),
                        abs(fuzzyUsers - exactUsers) <= tolerance);
            }

            final SearchResponse response2 = getHistogram(__days[0], __days[7], "day", __txtField, 1000);
            final InternalDistinctDateHistogramFacet facet2 = response2.getFacets().facet(__facetName);
            final List<DistinctEntry> facetList2 = facet2.entries();
            assertEquals(7, facetList2.size());
            for(int i = 0; i < 7; i++) {
                final int exactTokens = itemsPerDay[i] * 3; // "Document created [by] <ID>"
                final int exactDistinctTokens = itemsPerDay[i] + 2;
                assertEquals(exactTokens, facetList2.get(i).getTotalCount());
                final int tolerance = exactDistinctTokens / 100;
                final long fuzzyDistinctTokens = facetList2.get(i).getDistinctCount();
                //System.out.println("Exact distinct token count = " + exactDistinctTokens);
                //System.out.println("Fuzzy distinct token count = " + fuzzyDistinctTokens);
                assertTrue(String.format(
                        "With > %d terms per day: Estimated count %d is not within 1%% tolerance of %d",
                        minPerDay, fuzzyDistinctTokens, exactDistinctTokens),
                        abs(fuzzyDistinctTokens - exactDistinctTokens) <= tolerance);
            }
        }
    }

    // Helper methods

    private static int newID() {
        return __counter.getAndIncrement();
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
                        .interval(interval);
        return client().prepareSearch(__index)
                .setSearchType(SearchType.COUNT)
                .addFacet(facet)
                .execute()
                .actionGet();
    }

    private void putSync(final int id, final int user, final long timestamp) throws ElasticSearchException, IOException {
        final String stringID = String.valueOf(id);
        client().prepareIndex(__index, __type, String.valueOf(stringID))
                .setRefresh(true)
                .setRouting(stringID)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field(__txtField, "Document created at " + timestamp)
                        .field(__userField, user)
                        .field(__tsField, timestamp)
                        .endObject()).execute().actionGet();
    }

    private void putBulk(final String[] ids, final int[] users, final long[] timestamps) throws Exception {
        final int batchSize = 5000;
        for(int i = 0; i < ids.length; i += batchSize) {
            final BulkRequestBuilder bulk = client().prepareBulk();
            for(int j = 0; j < batchSize; j++) {
                final int idx = i + j;
                if(idx >= ids.length) {
                    bulk.setRefresh(true).execute().actionGet();
                    return;
                }
                bulk.add(new IndexRequest(__index, __type, ids[idx])
                        .routing(ids[idx])
                        .source(XContentFactory.jsonBuilder()
                                .startObject()
                                .field(__txtField, "Document created by " + users[idx])
                                .field(__userField, users[idx])
                                .field(__tsField, timestamps[idx])
                                .endObject()));
            }
            bulk.execute().actionGet();
        }
        new RefreshRequestBuilder(client().admin().indices()).execute();
    }

    private int[] prepareRandomData(final int minPerDay) throws Exception {
        final int[] itemsPerDay = new int[7];
        final int variationPerDay = max(1, minPerDay / 10);
        for(int i = 0; i < 7; i++) {
            itemsPerDay[i] = minPerDay + _random.nextInt(variationPerDay);
            final int[] ids = new int[itemsPerDay[i]];
            final String[] stringIDs = new String[itemsPerDay[i]];
            final long[] timestamps = new long[itemsPerDay[i]];
            for(int j = 0; j < itemsPerDay[i]; j++) {
                timestamps[j] = __days[i] + (60 * 1000 * (int) (random() * 1440));
                ids[j] = newID();
                stringIDs[j] = String.valueOf(ids[j]);
            }
            putBulk(stringIDs, ids, timestamps);
        }
        client().admin().indices().optimize(
                new OptimizeRequest().waitForMerge(true));
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
                .getCount();
    }

    private Client client() {
        return __node.client();
    }

}
