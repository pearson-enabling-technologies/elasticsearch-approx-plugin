package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.google.common.collect.Lists.newArrayList;
import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.RANDOM;
import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomInts;
import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomLongs;
import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomWords;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TermListFacetTest {

    private static Node __node;

    private static final String __index = "myindex";

    private static final String __type = "testtype";

    private static final String __txtField1 = "txt1";
    private static final String __txtField2 = "txt2";
    private static final String __intField1 = "int1";
    private static final String __longField1 = "long1";

    private static final String __facetName = "term_list_facet";

    private static final AtomicInteger __counter = new AtomicInteger(0);

    @BeforeClass
    public static void setUpClass() {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                // Reluctantly removed this to reduce overall memory:
                //.put("index.store.type", "memory")
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("path.data", "target")
                .put("refresh_interval", -1)
                .put("index.cache.field.type", "soft")
                .build();
        __node = nodeBuilder()
                .local(true)
                .settings(settings)
                .clusterName("TermListFacetTest")
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
                .startObject(__txtField1).field("type", "string").field("store", "no").endObject()
                .startObject(__txtField2).field("type", "string").field("store", "no").endObject()
                .startObject(__intField1).field("type", "integer").field("store", "yes").field("index", "not_analyzed").endObject()
                .startObject(__longField1).field("type", "long").field("store", "no").endObject()
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
    public void testWithFixedVocabulary() throws Exception {

        final String[] _words = { "foo", "bar", "baz", "test", "alpha", "beta", "phi", "rho" };
        final List<String> words = new ArrayList<String>();
        for(final String word : _words)
            words.add(word);

        final int numOfDocs = _words.length;

        for(int i = 0; i < _words.length; i++) {
            putSync(newID(), _words[i], _words[i], 0, 0);
        }

        final Set<String> uniqs = new HashSet<String>(Arrays.asList(_words));

        assertEquals(numOfDocs, countAll());
        final SearchResponse response1 = getTermList("src/test/resources/TermListFacetTest.json");
        checkStringSearchResponse(response1, numOfDocs, uniqs.size(), words);
    }

    @Test
    public void testWithFixedIntegersPostMode() throws Exception {
        testWithFixedIntegers(Constants.POST_MODE);
    }

    @Test
    public void testWithFixedIntegersCollectorMode() throws Exception {
        testWithFixedIntegers(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testWithRandomStringsCollectorMode() throws Exception {
        testWithRandomStrings(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testIntsPostMode() throws Exception {
        testInts(Constants.POST_MODE);
    }

    @Test
    public void testIntsColectorMode() throws Exception {
        testInts(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testWithRandomStringsPostMode() throws Exception {
        testWithRandomStrings(Constants.POST_MODE);
    }

    @Test
    public void testLongsPostMode() throws Exception {
        testLongs(Constants.POST_MODE);
    }

    @Test
    public void testLongsCollectorMode() throws Exception {
        testLongs(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testWithJsonWithRandomStringsCollectorMode() throws Exception {
        testWithJsonSettings("src/test/resources/TermListFacetTest.json");
    }

    @Test
    public void testWithJsonWithRandomStringsPostMode() throws Exception {
        testWithJsonSettings("src/test/resources/TermListFacetTestPostMode.json");
    }

    @Test
    public void testWithIntRandomDataCollectorMode() throws Exception {
        testWithIntRandomData(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testWithIntRandomDataPostMode() throws Exception {
        testWithIntRandomData(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testWithLongRandomDataPostMode() throws Exception {
        testWithLongRandomData(Constants.POST_MODE);
    }

    @Test
    public void testWithLongRandomDataCollectorMode() throws Exception {
        testWithLongRandomData(Constants.COLLECTOR_MODE);
    }

    @Test
    public void testAllFieldsWithRandomValuesSampled() throws Exception {
        testAllFieldsWithRandomValues("Sampled", 0.1f);
    }

    @Test
    public void testAllFieldsWithRandomValuesExhaustive() throws Exception {
        testAllFieldsWithRandomValues("Exact", 1);
    }

    // Helper methods

    private void testWithFixedIntegers(final String mode) throws Exception {
        final int[] _words = { 0, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
        final List<Integer> words = new ArrayList<Integer>();
        for(final int word : _words)
            words.add(word);

        final int numOfDocs = _words.length;

        for(int i = 0; i < _words.length; i++) {
            putSync(newID(), "", "", _words[i], _words[i]);
        }

        final Set<Integer> uniqs = new HashSet<Integer>();
        uniqs.addAll(words);

        assertEquals(numOfDocs, countAll());
        final SearchResponse response1 = getTermList(__intField1, _words.length, 1, mode);
        checkIntSearchResponse(response1, numOfDocs, uniqs.size(), words);
    }

    private void testWithRandomStrings(final String mode) throws Exception {
        final int numOfElements = 100;
        final int numOfWords = 100;
        final List<String> words = generateRandomWords(numOfWords);

        int rIndex1 = RANDOM.nextInt(numOfWords);
        int rIndex2 = RANDOM.nextInt(numOfWords);
        for(int i = 0; i < numOfElements; i++) {
            putSync(newID(), words.get(rIndex1), words.get(rIndex2), 0, 0);
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;
        }

        final Set<String> uniqs = new HashSet<String>(words);

        assertEquals(numOfElements, countAll());
        final SearchResponse response1 = getTermList(__txtField1, numOfElements, 1, mode);
        final SearchResponse response2 = getTermList(__txtField2, numOfElements, 1, mode);

        checkStringSearchResponse(response1, numOfElements, uniqs.size(), words);
        checkStringSearchResponse(response2, numOfElements, uniqs.size(), words);
    }

    private void testInts(final String mode) throws Exception {

        final int testLength = 7;
        final int maxPerShard = 3;
        final List<Integer> numList = generateRandomInts(testLength);
        for(int i = 0; i < numList.size(); i++) {
            putSync(newID(), "", "", numList.get(i), 0);
        }
        final SearchResponse response1 = getTermList(__intField1, maxPerShard, 1, mode);
        checkIntSearchResponse(response1, testLength, testLength, numList);
    }

    private void testWithJsonSettings(final String file) throws ElasticSearchException, IOException {
        final int numOfElements = 100 + RANDOM.nextInt(100);
        final int numOfWords = 20 + RANDOM.nextInt(10);
        final List<String> words = generateRandomWords(numOfWords);

        int rIndex1 = RANDOM.nextInt(numOfWords);
        int rIndex2 = RANDOM.nextInt(numOfWords);
        for(int i = 0; i < numOfElements; i++) {
            putSync(newID(), words.get(rIndex1), words.get(rIndex2), 0, 0);
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;
        }

        final Set<String> uniqs = new HashSet<String>(words);

        assertEquals(numOfElements, countAll());
        final SearchResponse response1 = getTermList(file);
        checkStringSearchResponse(response1, numOfElements, uniqs.size(), words);
    }

    private void testWithIntRandomData(final String mode) throws Exception {

        final int numOfDocumentsToIndex = 100; //200 + RANDOM.nextInt(200);
        final int numOfWordsToGenerate = 100; //100 + RANDOM.nextInt(100);

        final List<Integer> nums = generateRandomInts(numOfWordsToGenerate);
        final Set<Integer> uniqs = new HashSet<Integer>(nums);

        int rIndex = RANDOM.nextInt(numOfWordsToGenerate);

        for(int i = 0; i < numOfDocumentsToIndex; i++) {

            putSync(newID(), "", "", nums.get(rIndex), 0);
            rIndex++;
            rIndex %= numOfWordsToGenerate;

        }
        final SearchResponse response1 = getTermList(__intField1, numOfWordsToGenerate, 1, mode);
        checkIntSearchResponse(response1, numOfDocumentsToIndex, uniqs.size(), nums);
    }

    private void testWithLongRandomData(final String mode) throws Exception {

        final int numOfDocumentsToIndex = 200 + RANDOM.nextInt(200);
        final int numOfWordsToGenerate = 100 + RANDOM.nextInt(100);

        final List<Long> nums = generateRandomLongs(numOfWordsToGenerate);
        final Set<Long> uniqs = new HashSet<Long>(nums);

        int rIndex2 = RANDOM.nextInt(numOfWordsToGenerate);

        for(int i = 0; i < numOfDocumentsToIndex; i++) {

            putSync(newID(), "", "", 0, nums.get(rIndex2));

            rIndex2++;
            rIndex2 %= numOfWordsToGenerate;

        }
        final SearchResponse response1 = getTermList(__longField1, numOfWordsToGenerate, 1, mode);
        checkLongSearchResponse(response1, numOfDocumentsToIndex, uniqs.size(), nums);

    }

    private void testAllFieldsWithRandomValues(final String label, final float sample) throws Exception {
        final int numOfElements = 10000;// + _random.nextInt(100);
        final int numOfWords = 100;// + _random.nextInt(10);
        final List<String> words = generateRandomWords(numOfWords);
        final List<Integer> ints = generateRandomInts(numOfWords);
        final List<Long> longs = generateRandomLongs(numOfWords);

        int rIndex1 = 0; //_random.nextInt(numOfWords);
        int rIndex2 = 1;//_random.nextInt(numOfWords);
        int rIndex3 = 2;//_random.nextInt(numOfWords);
        int rIndex4 = 3; //_random.nextInt(numOfWords);

        for(int i = 0; i < numOfElements; i++) {
            addToBulk(newID(), words.get(rIndex1), words.get(rIndex2), ints.get(rIndex3), longs.get(rIndex4));
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;

            rIndex3++;
            rIndex3 %= numOfWords;

            rIndex4++;
            rIndex4 %= numOfWords;
        }
        sendBulk();
        Thread.sleep(2000);

        final Set<String> uniqsStrings = new HashSet<String>(words);
        final Set<Integer> uniqInts = new HashSet<Integer>(ints);
        final Set<Long> uniqLongs = new HashSet<Long>(longs);

        SearchResponse response1 = null;
        SearchResponse response2 = null;
        SearchResponse response3 = null;
        SearchResponse response4 = null;
        assertEquals(numOfElements, countAll());
        clearMemory();
        final long start = System.currentTimeMillis();
        for(int i = 0; i < 2000; i++) {
            response1 = getTermList(__txtField1, numOfElements, sample, Constants.COLLECTOR_MODE);
            response2 = getTermList(__txtField2, numOfElements, sample, Constants.COLLECTOR_MODE);
            response3 = getTermList(__intField1, numOfElements, sample, Constants.COLLECTOR_MODE);
            response4 = getTermList(__longField1, numOfElements, sample, Constants.COLLECTOR_MODE);
        }
        System.out.println(label + " queries ran in " + (System.currentTimeMillis() - start) + " ms");

        checkStringSearchResponse(response1, numOfElements, uniqsStrings.size(), words);
        checkStringSearchResponse(response2, numOfElements, uniqsStrings.size(), words);
        checkIntSearchResponse(response3, numOfElements, uniqInts.size(), ints);
        checkLongSearchResponse(response4, numOfElements, uniqLongs.size(), longs);

    }

    private void testLongs(final String mode) throws Exception {

        final int testLength = 7;
        final int maxPerShard = 3;
        final List<Long> numList = generateRandomLongs(testLength);
        for(int i = 0; i < numList.size(); i++) {
            putSync(newID(), "", "", 1, numList.get(i));
        }
        final SearchResponse response1 = getTermList(__longField1, maxPerShard, 1, mode);
        checkLongSearchResponse(response1, testLength, testLength, numList);
    }

    private static int newID() {
        return __counter.getAndIncrement();
    }

    private SearchResponse getTermList(final String valueField, final int maxPerShard, final float sample, final String mode) {

        final TermListFacetBuilder facet =
                new TermListFacetBuilder(__facetName)
                        .keyField(valueField)
                        .maxPerShard(maxPerShard)
                        .mode(mode)
                        .sample(sample);

        return client().prepareSearch(__index)
                .setSearchType(SearchType.COUNT)
                .addFacet(facet)
                .execute().actionGet();
    }

    private SearchResponse getTermList(final String jsonFilename) throws FileNotFoundException {
        return client().prepareSearch(__index)
                .setSource(new Scanner(new File(jsonFilename)).useDelimiter("\\Z").next())
                .execute()
                .actionGet();
    }

    private void putSync(final int id, final String value1, final String value2,
            final int iValue1, final long lValue)
            throws ElasticSearchException,
            IOException {
        final String stringID = String.valueOf(id);
        client().prepareIndex(__index, __type, String.valueOf(stringID))
                .setRefresh(true)
                .setRouting(stringID)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field(__txtField1, value1)
                        .field(__txtField2, value2)
                        .field(__intField1, iValue1)
                        .field(__longField1, lValue)
                        .endObject()).execute().actionGet();
    }

    private final List<IndexRequest> _bulkBuffer = newArrayList();

    private void addToBulk(final int id, final String value1, final String value2,
            final int iValue1, final long lValue) throws IOException {
        final String stringID = String.valueOf(id);
        _bulkBuffer.add(new IndexRequest(__index, __type, stringID)
                .routing(stringID).source(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field(__txtField1, value1)
                                .field(__txtField2, value2)
                                .field(__intField1, iValue1)
                                .field(__longField1, lValue)
                                .endObject()));
    }

    private void sendBulk() {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for(final IndexRequest req : _bulkBuffer) {
            bulk.add(req);
        }
        bulk.setRefresh(true).execute().actionGet();
        bulk.execute().actionGet();
    }

    private void checkStringSearchResponse(final SearchResponse sr, final int numOfDocs, final int numOfElements, final List<String> words) {

        assertEquals(numOfDocs, sr.getHits().getTotalHits());
        final TermListFacet facet = sr.getFacets().facet(__facetName);
        final ArrayList<String> facetList = newArrayList(facet);
        final List<? extends String> entries = facet.getEntries();
        final int len = facetList.size();
        assertEquals(numOfElements, len);
        for(final Object item : entries) {
            assertTrue(words.contains(item.toString()));
        }

    }

    private void checkIntSearchResponse(final SearchResponse sr, final int numOfReturnedDocs, final int numOfReturnedFacetElements, final List<Integer> ints) {

        assertEquals(numOfReturnedDocs, sr.getHits().getTotalHits());
        final TermListFacet facet = sr.getFacets().facet(__facetName);
        final ArrayList<String> facetList = newArrayList(facet);
        final List<? extends Object> entries = facet.getEntries();
        final int len = facetList.size();

        assertEquals(numOfReturnedFacetElements, len);
        for(final Object item : entries) {
            final int t = Integer.parseInt(item.toString());
            assertTrue(ints.contains(t));
        }
    }

    private void checkLongSearchResponse(final SearchResponse sr, final int numOfDocs, final int numOfElements, final List<Long> longs) {

        assertEquals(numOfDocs, sr.getHits().getTotalHits());
        final TermListFacet facet = sr.getFacets().facet(__facetName);
        final ArrayList<String> facetList = newArrayList(facet);

        final int len = facetList.size();
        assertEquals(numOfElements, len);

        for(final Object item : facetList) {
            final Long val = Long.parseLong(item.toString());
            assertTrue(longs.contains(val));
        }
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

    private void clearMemory() throws Exception {
        client().admin().indices().prepareClearCache(__index).execute().actionGet();
        System.gc();
        Thread.sleep(2000);
    }

}
