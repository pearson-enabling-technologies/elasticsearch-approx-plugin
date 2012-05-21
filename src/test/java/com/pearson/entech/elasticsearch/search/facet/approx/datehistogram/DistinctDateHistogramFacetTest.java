package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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

    private static final String __idField = "_id";

    private static final String __facetName = "histogram";

    @BeforeClass
    public static void setUpClass() {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("path.data", "target").build();
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
    public void testWithMaxOneTotalValuePerBucket() throws Exception {
        putSync("0", __days[0] + 10);
        putSync("2", __days[2] + 10);
        putSync("3", __days[4] + 10);
        putSync("6", __days[6] + 10);
        assertEquals(4, countAll());
        //Thread.sleep(Long.MAX_VALUE);
        final SearchResponse response = getHistogram(__days[0], __days[7], "day");
        assertEquals(4, response.hits().getTotalHits());
        final DistinctDateHistogramFacet facet = response.facets().facet(__facetName);
        final ArrayList<Entry> facetList = newArrayList(facet);
        assertEquals(4, facetList.size());
        assertEquals(__days[0], facetList.get(0).getTime());
        assertEquals(__days[2], facetList.get(1).getTime());
        assertEquals(__days[4], facetList.get(2).getTime());
        assertEquals(__days[6], facetList.get(3).getTime());
        assertEquals(1, facetList.get(0).count());
        assertEquals(1, facetList.get(1).count());
        assertEquals(1, facetList.get(2).count());
        assertEquals(1, facetList.get(3).count());
        assertEquals(1, facetList.get(0).distinctCount());
        assertEquals(1, facetList.get(1).distinctCount());
        assertEquals(1, facetList.get(2).distinctCount());
        assertEquals(1, facetList.get(3).distinctCount());
    }

    // Helper methods

    private SearchResponse getHistogram(final long start, final long end, final String interval) {
        final FilterBuilder range =
                FilterBuilders.numericRangeFilter(__tsField)
                        .from(start)
                        .to(end);
        // TODO WTF is there a "_uid" and not an "_id" in the indexed docs?
        final DistinctDateHistogramFacetBuilder facet =
                new DistinctDateHistogramFacetBuilder(__facetName)
                        .keyField(__tsField)
                        .valueField("txt")
                        .facetFilter(range)
                        .interval(interval);
        return client().prepareSearch(__index)
                .setSearchType(SearchType.COUNT)
                .addFacet(facet)
                .execute().actionGet();
    }

    private void putSync(final String id, final long timestamp) throws ElasticSearchException, IOException {
        client().prepareIndex(__index, __type, id)
                .setRefresh(true)
                .setRouting(id)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field(__txtField, "Document created at " + timestamp)
                        .field(__tsField, timestamp)
                        .endObject()).execute().actionGet();
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
