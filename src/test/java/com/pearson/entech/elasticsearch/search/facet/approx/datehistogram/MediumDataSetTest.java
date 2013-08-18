package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
import java.util.Scanner;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public class MediumDataSetTest {

    private static Node __node;

    private final String _table = "testtable_20130506";

    private final String _type = "*";

    private final Random _random = new Random(0);

    private final String _distinctExactDir = "src/test/resources/distinct_exact/";

    @BeforeClass
    public static void setUpClass() throws Exception {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", true)
                .put("index.number_of_replicas", 0)
                .put("path.data", "/Users/andrcleg/ElasticSearch/elasticsearch-0.20.6/data")
                .build();
        __node = nodeBuilder()
                .local(true)
                .settings(settings)
                .clusterName("MediumDataSetTest")
                .node();
        __node.start();
        __node.client().admin().cluster().prepareHealth()
                .setWaitForGreenStatus().execute().actionGet();
        //        Thread.sleep(5000);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        __node.stop();
    }

    @Test
    public void testMinuteIntervalUnboundedStringExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "minute_interval_unbounded_string");
    }

    @Test
    public void testMinuteIntervalUnboundedLongExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "minute_interval_unbounded_long");
    }

    @Test
    public void testDayIntervalLondonUnboundedExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "day_interval_london_unbounded_boolean");
    }

    @Test
    public void testDayIntervalKolkataUnboundedExact() throws Exception {
        compareHitsAndFacets(_distinctExactDir + "day_interval_kolkata_unbounded_boolean");
    }

    private void compareHitsAndFacets(final String fileStem) throws Exception {
        final String reqFileName = fileStem + "-REQUEST.json";
        final String respFileName = fileStem + "-RESPONSE.json";
        final JSONObject response = jsonRequest(_table, _type, reqFileName);
        final JSONObject expected = getJsonFile(respFileName);
        compare(expected, response, "hits", "facets");
    }

    private void compare(final JSONObject expected, final JSONObject response, final String... fields) throws Exception {
        for(final String field : fields) {
            final JSONObject exp = expected.getJSONObject(field);
            final JSONObject resp = response.getJSONObject(field);
            try {
                JSONAssert.assertEquals(exp, resp, true);
            } catch(final AssertionError e) {
                System.out.println("Expected: " + exp);
                System.out.println("Received: " + resp);
                throw(e);
            }
        }
    }

    private JSONObject jsonRequest(final String index, final String type, final String filename) throws Exception {
        final SearchResponse response = client().prepareSearch(index)
                .setSource(getFile(filename))
                .setSearchType(SearchType.COUNT)
                .execute()
                .actionGet();
        return new JSONObject(response.toString());
    }

    private Client client() {
        return __node.client();
    }

    private String getFile(final String filename) throws FileNotFoundException {
        return new Scanner(new File(filename)).useDelimiter("\\Z").next();
    }

    private JSONObject getJsonFile(final String filename) throws Exception {
        return new JSONObject(getFile(filename));
    }
}
