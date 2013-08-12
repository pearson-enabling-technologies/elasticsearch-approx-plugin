package com.pearson.entech.elasticsearch.plugin.approx;

import static com.google.common.collect.Lists.newArrayList;
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
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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

import com.pearson.entech.elasticsearch.search.facet.approx.termlist.TermListFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.termlist.TermListFacetBuilder;

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
    public void testWithFixedIntegers() throws Exception {

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
        final SearchResponse response1 = getTermList(__intField1, _words.length, true);

        checkIntSearchResponse(response1, numOfDocs, uniqs.size(), words);
    }

    @Test
    public void testWithJsonWithRandomStringsNoCache() throws Exception {
        final int numOfElements = 100 + _random.nextInt(100);
        final int numOfWords = 20 + _random.nextInt(10);
        final List<String> words = generateRandomWords(numOfWords);

        int rIndex1 = _random.nextInt(numOfWords);
        int rIndex2 = _random.nextInt(numOfWords);
        for(int i = 0; i < numOfElements; i++) {
            putSync(newID(), words.get(rIndex1), words.get(rIndex2), 0, 0);
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;
        }

        final Set<String> uniqs = new HashSet<String>(words);

        assertEquals(numOfElements, countAll());
        final SearchResponse response1 = getTermList("src/test/resources/TermListFacetTest.json");

        System.out.println(response1.toString());

        checkStringSearchResponse(response1, numOfElements, uniqs.size(), words);
    }

    @Test
    public void testWithRandomStringsNoCache() throws Exception {
        final int numOfElements = 100 + _random.nextInt(100);
        final int numOfWords = 20 + _random.nextInt(10);
        final List<String> words = generateRandomWords(numOfWords);

        int rIndex1 = _random.nextInt(numOfWords);
        int rIndex2 = _random.nextInt(numOfWords);
        for(int i = 0; i < numOfElements; i++) {
            putSync(newID(), words.get(rIndex1), words.get(rIndex2), 0, 0);
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;
        }

        final Set<String> uniqs = new HashSet<String>(words);

        assertEquals(numOfElements, countAll());
        final SearchResponse response1 = getTermList(__txtField1, numOfElements, false);
        final SearchResponse response2 = getTermList(__txtField2, numOfElements, false);

        checkStringSearchResponse(response1, numOfElements, uniqs.size(), words);
        checkStringSearchResponse(response2, numOfElements, uniqs.size(), words);

    }

    @Test
    public void testWithRandomStringsWithCache() throws Exception {
        final int numOfElements = 100 + _random.nextInt(100);
        final int numOfWords = 20 + _random.nextInt(10);
        final List<String> words = generateRandomWords(numOfWords);

        int rIndex1 = _random.nextInt(numOfWords);
        int rIndex2 = _random.nextInt(numOfWords);
        for(int i = 0; i < numOfElements; i++) {
            putSync(newID(), words.get(rIndex1), words.get(rIndex2), 0, 0);
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;
        }

        final Set<String> uniqs = new HashSet<String>(words);

        assertEquals(numOfElements, countAll());
        final SearchResponse response1 = getTermList(__txtField1, numOfElements, true);
        final SearchResponse response2 = getTermList(__txtField2, numOfElements, true);

        checkStringSearchResponse(response1, numOfElements, uniqs.size(), words);
        checkStringSearchResponse(response2, numOfElements, uniqs.size(), words);

    }

    @Test
    public void testLongsNoCache() throws Exception {

        final int testLength = 7;
        final int maxPerShard = 3;
        final List<Long> numList = generateRandomLongs(testLength);
        for(int i = 0; i < numList.size(); i++) {
            putSync(newID(), "", "", 1, numList.get(i));
        }
        final SearchResponse response1 = getTermList(__longField1, maxPerShard, false);
        checkLongSearchResponse(response1, testLength, testLength, numList);

    }

    @Test
    public void testIntsNoCache() throws Exception {

        final int testLength = 7;
        final int maxPerShard = 3;
        final List<Integer> numList = generateRandomInts(testLength);
        for(int i = 0; i < numList.size(); i++) {
            putSync(newID(), "", "", numList.get(i), 0);
        }
        final SearchResponse response1 = getTermList(__intField1, maxPerShard, false);
        checkIntSearchResponse(response1, testLength, testLength, numList);

    }

    @Test
    public void testWithIntRandomDataNoCache() throws Exception {

        final int numOfDocumentsToIndex = 200 + _random.nextInt(200);
        final int numOfWordsToGenerate = 100 + _random.nextInt(100);

        final List<Integer> nums = generateRandomInts(numOfWordsToGenerate);
        final Set<Integer> uniqs = new HashSet<Integer>(nums);

        int rIndex = _random.nextInt(numOfWordsToGenerate);

        for(int i = 0; i < numOfDocumentsToIndex; i++) {

            putSync(newID(), "", "", nums.get(rIndex), 0);
            rIndex++;
            rIndex %= numOfWordsToGenerate;

        }
        final SearchResponse response1 = getTermList(__intField1, numOfWordsToGenerate, false);
        checkIntSearchResponse(response1, numOfDocumentsToIndex, uniqs.size(), nums);

    }

    @Test
    public void testWithIntRandomDataWithCache() throws Exception {

        final int numOfDocumentsToIndex = 200 + _random.nextInt(200);
        final int numOfWordsToGenerate = 100 + _random.nextInt(100);

        final List<Integer> nums = generateRandomInts(numOfWordsToGenerate);
        final Set<Integer> uniqs = new HashSet<Integer>(nums);

        int rIndex = _random.nextInt(numOfWordsToGenerate);

        for(int i = 0; i < numOfDocumentsToIndex; i++) {

            putSync(newID(), "", "", nums.get(rIndex), 0);
            rIndex++;
            rIndex %= numOfWordsToGenerate;

        }
        final SearchResponse response1 = getTermList(__intField1, numOfWordsToGenerate, true);
        checkIntSearchResponse(response1, numOfDocumentsToIndex, uniqs.size(), nums);
    }

    @Test
    public void testWithLongRandomDataNoCache() throws Exception {

        final int numOfDocumentsToIndex = 200 + _random.nextInt(200);
        final int numOfWordsToGenerate = 100 + _random.nextInt(100);

        final List<Long> nums = generateRandomLongs(numOfWordsToGenerate);
        final Set<Long> uniqs = new HashSet<Long>(nums);

        int rIndex2 = _random.nextInt(numOfWordsToGenerate);

        for(int i = 0; i < numOfDocumentsToIndex; i++) {

            putSync(newID(), "", "", 0, nums.get(rIndex2));

            rIndex2++;
            rIndex2 %= numOfWordsToGenerate;

        }
        final SearchResponse response1 = getTermList(__longField1, numOfWordsToGenerate, false);
        checkLongSearchResponse(response1, numOfDocumentsToIndex, uniqs.size(), nums);

    }

    @Test
    public void testWithLongRandomDataWithCache() throws Exception {

        final int numOfDocumentsToIndex = 200 + _random.nextInt(200);
        final int numOfWordsToGenerate = 100 + _random.nextInt(100);

        final List<Long> nums = generateRandomLongs(numOfWordsToGenerate);
        final Set<Long> uniqs = new HashSet<Long>(nums);

        int rIndex2 = _random.nextInt(numOfWordsToGenerate);

        for(int i = 0; i < numOfDocumentsToIndex; i++) {

            putSync(newID(), "", "", 0, nums.get(rIndex2));

            rIndex2++;
            rIndex2 %= numOfWordsToGenerate;

        }
        final SearchResponse response1 = getTermList(__longField1, numOfWordsToGenerate, true);
        checkLongSearchResponse(response1, numOfDocumentsToIndex, uniqs.size(), nums);

    }

    @Test
    public void testAllFieldsWithRandomValues() throws Exception {
        final int numOfElements = 300;// + _random.nextInt(100);
        final int numOfWords = 30;// + _random.nextInt(10);
        final List<String> words = generateRandomWords(numOfWords);
        final List<Integer> ints = generateRandomInts(numOfWords);
        final List<Long> longs = generateRandomLongs(numOfWords);

        int rIndex1 = 0; //_random.nextInt(numOfWords);
        int rIndex2 = 1;//_random.nextInt(numOfWords);
        int rIndex3 = 2;//_random.nextInt(numOfWords);
        int rIndex4 = 3; //_random.nextInt(numOfWords);

        for(int i = 0; i < numOfElements; i++) {
            putSync(newID(), words.get(rIndex1), words.get(rIndex2), ints.get(rIndex3), longs.get(rIndex4));
            rIndex1++;
            rIndex1 %= numOfWords;

            rIndex2++;
            rIndex2 %= numOfWords;

            rIndex3++;
            rIndex3 %= numOfWords;

            rIndex4++;
            rIndex4 %= numOfWords;
        }

        final Set<String> uniqsStrings = new HashSet<String>(words);
        final Set<Integer> uniqInts = new HashSet<Integer>(ints);
        final Set<Long> uniqLongs = new HashSet<Long>(longs);

        assertEquals(numOfElements, countAll());
        final SearchResponse response1 = getTermList(__txtField1, numOfElements, false);
        final SearchResponse response2 = getTermList(__txtField2, numOfElements, false);
        final SearchResponse response3 = getTermList(__intField1, numOfElements, false);
        final SearchResponse response4 = getTermList(__longField1, numOfElements, false);

        checkStringSearchResponse(response1, numOfElements, uniqsStrings.size(), words);
        checkStringSearchResponse(response2, numOfElements, uniqsStrings.size(), words);
        checkIntSearchResponse(response3, numOfElements, uniqInts.size(), ints);
        checkLongSearchResponse(response4, numOfElements, uniqLongs.size(), longs);

        assertEquals(numOfElements, countAll());
        final SearchResponse r1 = getTermList(__txtField1, numOfElements, true);
        final SearchResponse r2 = getTermList(__txtField2, numOfElements, true);
        final SearchResponse r3 = getTermList(__intField1, numOfElements, true);
        final SearchResponse r4 = getTermList(__longField1, numOfElements, true);

        checkStringSearchResponse(r1, numOfElements, uniqsStrings.size(), words);
        checkStringSearchResponse(r2, numOfElements, uniqsStrings.size(), words);
        checkIntSearchResponse(r3, numOfElements, uniqInts.size(), ints);
        checkLongSearchResponse(r4, numOfElements, uniqLongs.size(), longs);

    }

    List<Integer> generateRandomInts(final int numOfElements) {
        final List<Integer> ret = newArrayList();
        for(int i = 0; i < numOfElements; i++) {
            ret.add(_random.nextInt(1000));
        }
        return ret;
    }

    List<Long> generateRandomLongs(final int numOfElements) {
        final List<Long> ret = newArrayList();
        for(int i = 0; i < numOfElements; i++) {
            final long val = _random.nextInt(10000);
            ret.add(val);
        }
        return ret;
    }

    // Helper methods

    private static int newID() {
        return __counter.getAndIncrement();
    }

    private SearchResponse getTermList(final String valueField, final int maxPerShard, final boolean readFromCache) {

        final TermListFacetBuilder facet =
                new TermListFacetBuilder(__facetName)
                        .keyField(valueField)
                        .maxPerShard(maxPerShard).
                        readFromCache(readFromCache);

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

    private void putSync(final int id, final String value1, final String value2, final int iValue1, final long lValue) throws ElasticSearchException,
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

    private List<String> generateRandomWords(final int numberOfWords) {
        final String[] randomStrings = new String[numberOfWords];
        for(int i = 0; i < numberOfWords; i++)
        {
            final char[] word = new char[_random.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for(int j = 0; j < word.length; j++)
            {
                word[j] = (char) ('a' + _random.nextInt(26));
            }
            randomStrings[i] = new String(word);
        }
        return newArrayList(randomStrings);
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
