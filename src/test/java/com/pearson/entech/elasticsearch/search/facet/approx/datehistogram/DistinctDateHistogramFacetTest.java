package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.pow;
import static java.lang.Math.random;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.facet.FacetBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;

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

    private static final String __type1 = "testtype";

    private static final String __type2 = "anothertesttype";

    private static final String __type3 = "yetanothertesttype";

    private static final String __tsField = "timestamp";

    private static final String __txtField = "txt";

    private static final String __userField = "user";

    private static final String __facetName = "histogram";

    private static final AtomicInteger __counter = new AtomicInteger(0);

    private final Random _random = new Random(0);

    @BeforeClass
    public static void setUpClass() throws InterruptedException {
        final Settings settings = ImmutableSettings.settingsBuilder()
                //.put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                // Reluctantly removed this to reduce overall memory:
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.merge.policy.merge_factor", 50)
                .put("path.data", "target")
                .put("refresh_interval", -1)
                .build();
        __node = nodeBuilder()
                .local(true)
                .settings(settings)
                .clusterName("DistinctDateHistogramFacetTest")
                .node();
        __node.start();
        //Thread.sleep(30000);
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
                .startObject(__type1)
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
                .setType(__type1)
                .setSource(mapping)
                .execute().actionGet();
        client().admin().indices().clearCache(
                new ClearIndicesCacheRequest("_all"));
        System.gc();
        assertEquals(0L, countAll());
    }

    @Test
    public void testDateHistoFacetsCollectorMode() throws Exception {
        testDateHistoFacets(FacetBuilder.Mode.COLLECTOR);
    }

    @Test
    public void testDateHistoFacetsPostMode() throws Exception {
        testDateHistoFacets(FacetBuilder.Mode.POST);
    }

    private void testDateHistoFacets(final FacetBuilder.Mode mode) throws Exception {
        // Tests pass whether or not fields are explicitly mapped
        //        final String mapping = jsonBuilder().startObject().startObject(__type2).startObject("properties")
        //                .startObject("num").field("type", "integer").endObject()
        //                .startObject("date").field("type", "date").endObject()
        //                .endObject().endObject().endObject().string();
        //        client().admin().indices().preparePutMapping(__index).setType(__type2).setSource(mapping).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex(__index, __type2).setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T01:01:01")
                .field("num", 1)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client().prepareIndex(__index, __type2).setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T04:01:01")
                .field("num", 2)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex(__index, __type2).setSource(jsonBuilder().startObject()
                .field("date", "2009-03-06T01:01:01")
                .field("num", 3)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        final SearchResponse searchResponse = client()
                .prepareSearch()
                .setQuery(matchAllQuery())
                .addFacet(new DistinctDateHistogramFacetBuilder("stats1").keyField("date").valueField("num").interval("day").mode(mode))
                .addFacet(new DistinctDateHistogramFacetBuilder("stats2").keyField("date").valueField("num").interval("day").preZone("-02:00").mode(mode))
                .addFacet(new DistinctDateHistogramFacetBuilder("stats3").keyField("date").valueField("num").interval("day").preZone("-02:00").mode(mode))
                //                .addFacet(
                //                        new DistinctDateHistogramFacetBuilder("stats4").keyField("date").valueScript("doc['num'].value * 2").interval("day").preZone("-02:00")
                //                                .mode(mode))
                .addFacet(new DistinctDateHistogramFacetBuilder("stats5").keyField("date").valueField("num").interval("24h").mode(mode))
                .addFacet(
                        new DistinctDateHistogramFacetBuilder("stats6").keyField("date").valueField("num").interval("day").preZone("-02:00").postZone("-02:00")
                                .mode(mode))
                .addFacet(new DistinctDateHistogramFacetBuilder("stats7").keyField("date").valueField("num").interval("quarter").mode(mode))
                .execute().actionGet();

        if(searchResponse.getFailedShards() > 0) {
            System.out.println(searchResponse); // TODO remove all printlns
            fail(Joiner.on(", ").join(searchResponse.getShardFailures()));
        }

        InternalDistinctFacet facet = searchResponse.getFacets().facet("stats1");
        assertThat(facet.getName(), equalTo("stats1"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(1l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));

        // time zone causes the dates to shift by 2
        facet = searchResponse.getFacets().facet("stats2");
        assertThat(facet.getName(), equalTo("stats2"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04")));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(2l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));

        // time zone causes the dates to shift by 2
        facet = searchResponse.getFacets().facet("stats3");
        assertThat(facet.getName(), equalTo("stats3"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04")));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(2l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));

        // time zone causes the dates to shift by 2
        //        facet = searchResponse.getFacets().facet("stats4");
        //        assertThat(facet.getName(), equalTo("stats4"));
        //        assertThat(facet.getEntries().size(), equalTo(2));
        //        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04")));
        //        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(1l));
        //        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(1l));
        //        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        //        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
        //        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(2l));

        facet = searchResponse.getFacets().facet("stats5");
        assertThat(facet.getName(), equalTo("stats5"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(1l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));

        facet = searchResponse.getFacets().facet("stats6");
        assertThat(facet.getName(), equalTo("stats6"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04") - TimeValue.timeValueHours(2).millis()));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05") - TimeValue.timeValueHours(2).millis()));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(2l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));

        facet = searchResponse.getFacets().facet("stats7");
        assertThat(facet.getName(), equalTo("stats7"));
        assertThat(facet.getEntries().size(), equalTo(1));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-01-01")));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/2141
    public void testDateHistoFacets_preZoneBug() throws Exception {
        // Tests pass whether or not fields are explicitly mapped
        //        final String mapping = jsonBuilder().startObject().startObject(__type3).startObject("properties")
        //                .startObject("num").field("type", "integer").endObject()
        //                .startObject("date").field("type", "date").endObject()
        //                .endObject().endObject().endObject().string();
        //        client().admin().indices().preparePutMapping(__index).setType(__type3).setSource(mapping).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex(__index, __type3).setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T23:31:01")
                .field("num", 1)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client().prepareIndex(__index, __type3).setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T18:01:01")
                .field("num", 2)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex(__index, __type3).setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T22:01:01")
                .field("num", 3)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        final SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addFacet(new DistinctDateHistogramFacetBuilder("stats1").keyField("date").valueField("num").interval("day").preZone("+02:00"))
                .addFacet(new DistinctDateHistogramFacetBuilder("stats2").keyField("date").valueField("num").interval("day").preZone("+01:30"))
                .execute().actionGet();

        if(searchResponse.getFailedShards() > 0) {
            System.out.println(searchResponse); // TODO remove all printlns
            fail(Joiner.on(", ").join(searchResponse.getShardFailures()));
        }

        // time zone causes the dates to shift by 2:00
        InternalDistinctFacet facet = searchResponse.getFacets().facet("stats1");
        assertThat(facet.getName(), equalTo("stats1"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(2l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));

        // time zone causes the dates to shift by 1:30
        facet = searchResponse.getFacets().facet("stats2");
        assertThat(facet.getName(), equalTo("stats2"));
        assertThat(facet.getEntries().size(), equalTo(2));
        assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
        assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
        assertThat(facet.getEntries().get(0).getDistinctCount(), equalTo(2l));
        assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
        assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
        assertThat(facet.getEntries().get(1).getDistinctCount(), equalTo(1l));
        assertThat(facet.getTotalCount(), equalTo(3l));
        assertThat(facet.getDistinctCount(), equalTo(3l));
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
        final InternalDistinctFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctTimePeriod<NullEntry>> facetList = facet.entries();
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
        assertThat(facet.getTotalCount(), equalTo(4l));
        assertThat(facet.getDistinctCount(), equalTo(1l)); // same user each time
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
        final InternalDistinctFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctTimePeriod<NullEntry>> facetList = facet.entries();
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
        assertThat(facet.getTotalCount(), equalTo(12l));
        assertThat(facet.getDistinctCount(), equalTo(6l)); // "document", "created", 4 usernames
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
        final InternalDistinctFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctTimePeriod<NullEntry>> facetList = facet.entries();
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
        assertThat(facet.getTotalCount(), equalTo(8l));
        assertThat(facet.getDistinctCount(), equalTo(4l)); // 4 different users
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
        final InternalDistinctFacet facet = response.getFacets().facet(__facetName);
        final List<DistinctTimePeriod<NullEntry>> facetList = facet.entries();
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
        assertThat(facet.getTotalCount(), equalTo(24l));
        assertThat(facet.getDistinctCount(), equalTo(10l)); // "document", "created", 8 usernames
    }

    @Test
    public void testRandomizedWithManyItemsOnDayBucket() throws Exception {

        // TODO test other data types

        // Do this 20 times for different amounts of data
        for(int t = 20; t <= 20; t++) {
            setUp();
            final int minPerDay = (int) pow(2, t);
            System.out.println("Randomized testing: inserting minimum " + 7 * minPerDay + " items");
            final int[] itemsPerDay = prepareRandomData(minPerDay);
            final int totalItems = add(itemsPerDay);
            assertEquals(totalItems, countAll());

            System.out.println("Randomized testing: running facet");
            final SearchResponse response = getHistogram(__days[0], __days[7], "day", __userField, 1000);
            final InternalDistinctFacet facet1 = response.getFacets().facet(__facetName);
            final List<DistinctTimePeriod<NullEntry>> facetList1 = facet1.entries();
            assertEquals(7, facetList1.size());
            assertEquals(totalItems, facet1.getTotalCount());
            int tolerance = totalItems / 100;
            int totalDistinct = totalItems;
            assertTrue(String.format(
                    "With %d total distinct items: Estimated overall distinct count %d is not within 1%% tolerance of %d",
                    totalDistinct, facet1.getDistinctCount(), totalDistinct),
                    abs(totalDistinct - facet1.getDistinctCount()) <= tolerance);
            for(int i = 0; i < 7; i++) {
                final int exactUsers = itemsPerDay[i];
                assertEquals(exactUsers, facetList1.get(i).getTotalCount());
                tolerance = exactUsers / 100;
                final long fuzzyUsers = facetList1.get(i).getDistinctCount();
                //System.out.println("Exact user count = " + exactUsers);
                //System.out.println("Fuzzy user count = " + fuzzyUsers);
                assertTrue(String.format(
                        "With > %d terms per day: Estimated count %d is not within 1%% tolerance of %d",
                        minPerDay, fuzzyUsers, exactUsers),
                        abs(fuzzyUsers - exactUsers) <= tolerance);
            }

            final SearchResponse response2 = getHistogram(__days[0], __days[7], "day", __txtField, 1000);
            final InternalDistinctFacet facet2 = response2.getFacets().facet(__facetName);
            final List<DistinctTimePeriod<NullEntry>> facetList2 = facet2.entries();
            assertEquals(7, facetList2.size());
            assertEquals(3 * totalItems, facet2.getTotalCount());
            tolerance = totalItems / 100;
            totalDistinct = 2 + totalItems;
            assertTrue(String.format(
                    "With %d total distinct items: Estimated overall distinct count %d is not within 1%% tolerance of %d",
                    totalDistinct, facet2.getDistinctCount(), totalDistinct),
                    abs(totalDistinct - facet2.getDistinctCount()) <= tolerance);
            for(int i = 0; i < 7; i++) {
                final int exactTokens = itemsPerDay[i] * 3; // "Document created [by] <ID>"
                final int exactDistinctTokens = itemsPerDay[i] + 2;
                assertEquals(exactTokens, facetList2.get(i).getTotalCount());
                tolerance = exactDistinctTokens / 100;
                final long fuzzyDistinctTokens = facetList2.get(i).getDistinctCount();
                //System.out.println("Exact distinct token count = " + exactDistinctTokens);
                //System.out.println("Fuzzy distinct token count = " + fuzzyDistinctTokens);
                assertTrue(String.format(
                        "With > %d terms per day: Estimated count %d is not within 1%% tolerance of %d",
                        minPerDay, fuzzyDistinctTokens, exactDistinctTokens),
                        abs(fuzzyDistinctTokens - exactDistinctTokens) <= tolerance);
            }
        }

        // TODO test total count/distinct
    }

    // Helper methods

    private static int newID() {
        return __counter.getAndIncrement();
    }

    private SearchResponse getHistogram(final long start, final long end, final String interval, final String valueField) {
        return getHistogram(start, end, interval, valueField, 0);
    }

    private SearchResponse getHistogram(final long start, final long end, final String interval, final String valueField, final int exactThreshold) {
        final FilterBuilder range =
                FilterBuilders.numericRangeFilter(__tsField)
                        .from(start)
                        .to(end);
        final DistinctDateHistogramFacetBuilder facet =
                new DistinctDateHistogramFacetBuilder(__facetName)
                        .keyField(__tsField)
                        .valueField(valueField)
                        .facetFilter(range)
                        .exactThreshold(exactThreshold)
                        .interval(interval);
        return client().prepareSearch(__index)
                .setSearchType(SearchType.COUNT)
                .addFacet(facet)
                .execute()
                .actionGet();
    }

    private void putSync(final int id, final int user, final long timestamp) throws ElasticSearchException, IOException {
        final String stringID = String.valueOf(id);
        client().prepareIndex(__index, __type1, String.valueOf(stringID))
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
                bulk.add(new IndexRequest(__index, __type1, ids[idx])
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

    private long utcTimeInMillis(final String time) {
        return timeInMillis(time, DateTimeZone.UTC);
    }

    private long timeInMillis(final String time, final DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }

    private Client client() {
        return __node.client();
    }

}
