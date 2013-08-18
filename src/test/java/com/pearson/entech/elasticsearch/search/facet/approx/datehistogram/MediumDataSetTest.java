package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
import java.util.Scanner;

import junit.framework.AssertionFailedError;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

public abstract class MediumDataSetTest {

    protected static Node __node;

    protected final static String _dataDir = "src/test/resources/data";

    protected final String _index = "testtable_20130506";

    protected final String _type = "*";

    protected final Random _random = new Random(0);

    protected final String _distinctExactDir = "src/test/resources/distinct_exact/";

    @BeforeClass
    public static void setUpClass() throws Exception {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", true)
                .put("index.number_of_replicas", 0)
                .put("path.data", _dataDir)
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
    public void testCorrectIndexAvailable() throws Exception {
        final int expectedSize = 489319;
        try {
            final long count = client()
                    .prepareCount(_index)
                    .execute()
                    .actionGet()
                    .getCount();
            if(count != expectedSize)
                throw new AssertionFailedError(count + " records in index.");
        } catch(final Exception e) {
            fail(String.format("Not found: index %s with %d records\n"
                    + "Download and install the test data for these tests from https://pearson.app.box.com/s/uvsz0gv8rhgex0aacc2u\n"
                    + "Reason for failure: %s",
                    _index, expectedSize, e.getMessage()));
        }
    }

    @Test
    @Ignore
    public void testBringUpServerForManualQuerying() throws Exception {
        Thread.sleep(10000000);
    }

    protected void compareHitsAndFacets(final String fileStem) throws Exception {
        final String reqFileName = fileStem + "-REQUEST.json";
        final String respFileName = fileStem + "-RESPONSE.json";
        final JSONObject response = jsonRequest(_index, _type, reqFileName);
        final JSONObject expected = getJsonFile(respFileName);
        compare(expected, response, "hits", "facets");
    }

    protected void compare(final JSONObject expected, final JSONObject response, final String... fields) throws Exception {
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

    protected JSONObject jsonRequest(final String index, final String type, final String filename) throws Exception {
        final SearchResponse response = client().prepareSearch(index)
                .setSource(getFile(filename))
                .setSearchType(SearchType.COUNT)
                .execute()
                .actionGet();
        return new JSONObject(response.toString());
    }

    protected Client client() {
        return __node.client();
    }

    protected String getFile(final String filename) throws FileNotFoundException {
        return new Scanner(new File(filename)).useDelimiter("\\Z").next();
    }

    protected JSONObject getJsonFile(final String filename) throws Exception {
        return new JSONObject(getFile(filename));
    }
}
