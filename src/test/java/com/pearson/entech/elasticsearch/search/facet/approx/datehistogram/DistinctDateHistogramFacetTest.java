package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class DistinctDateHistogramFacetTest {

    private static Node __node;

    private final String _index = "myindex";

    private final String _type = "testtype";

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
    public void setUp() {
        client().admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
        client().admin().indices().create(new CreateIndexRequest(_index)).actionGet();
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertEquals(0L, countAll());
    }

    // Helper methods

    private long countAll() {
        return client()
                .prepareCount(_index)
                .setQuery(wildcardQuery("_all", "*"))
                .execute()
                .actionGet()
                .count();
    }

    private Client client() {
        return __node.client();
    }

}
