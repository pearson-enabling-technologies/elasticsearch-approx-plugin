package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomInts;
import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomLongs;
import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomWords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.search.facet.InternalFacet;
import org.junit.Test;

public class SerializationTest {

    @Test
    public void testLongSerialization() throws Exception {
        final List<Long> randomInts = generateRandomLongs(1000);
        final BytesRefHash sentHash = new BytesRefHash();
        for(final Long word : randomInts) {
            sentHash.add(new BytesRef(word.toString()));
        }

        final InternalStringTermListFacet sentFacet = new InternalStringTermListFacet("foo", sentHash, Constants.FIELD_DATA_TYPE.INT);
        final InternalStringTermListFacet receivedFacet = new InternalStringTermListFacet();
        serializeAndDeserialize(sentFacet, receivedFacet);
        assertEquals("foo", receivedFacet.getName());
        final List<? extends String> entries = receivedFacet.getEntries();
        Collections.sort(randomInts);
        Collections.sort(entries);
        for(int i = 0; i < entries.size(); i++) {
            assertTrue(randomInts.contains(Long.parseLong(entries.get(i))));
        }
    }

    @Test
    public void testIntSerialization() throws Exception {
        final List<Integer> randomInts = generateRandomInts(1000);
        final BytesRefHash sentHash = new BytesRefHash();
        for(final Integer word : randomInts) {
            sentHash.add(new BytesRef(word.toString()));
        }

        final InternalStringTermListFacet sentFacet = new InternalStringTermListFacet("foo", sentHash, Constants.FIELD_DATA_TYPE.INT);
        final InternalStringTermListFacet receivedFacet = new InternalStringTermListFacet();
        serializeAndDeserialize(sentFacet, receivedFacet);
        assertEquals("foo", receivedFacet.getName());
        final List<? extends String> entries = receivedFacet.getEntries();
        Collections.sort(randomInts);
        Collections.sort(entries);
        //assertEquals( randomInts.size(), entries.size());
        for(int i = 0; i < entries.size(); i++) {
            assertTrue(randomInts.contains(Integer.parseInt(entries.get(i))));
        }

    }

    @Test
    public void testStringSerialization() throws Exception {
        final List<String> randomWords = generateRandomWords(1000);
        final BytesRefHash sentHash = new BytesRefHash();
        for(final String word : randomWords) {
            sentHash.add(new BytesRef(word));
        }

        final InternalStringTermListFacet sentFacet = new InternalStringTermListFacet("foo", sentHash, Constants.FIELD_DATA_TYPE.STRING);
        final InternalStringTermListFacet receivedFacet = new InternalStringTermListFacet();
        serializeAndDeserialize(sentFacet, receivedFacet);
        assertEquals("foo", receivedFacet.getName());
        final List<? extends String> entries = receivedFacet.getEntries();
        Collections.sort(randomWords);
        Collections.sort(entries);
        assertEquals(randomWords, entries);
    }

    // TODO this is a direct copy from SerializationTest for date facets
    private <T extends InternalFacet> void serializeAndDeserialize(final T toSend, final T toReceive) throws Exception {
        final BytesStreamOutput bso = new BytesStreamOutput();
        toSend.writeTo(bso);
        bso.close();
        final BytesReference bytes = bso.bytes();
        final BytesStreamInput bsi = new BytesStreamInput(bytes);
        toReceive.readFrom(bsi);
        bsi.close();
        assertEquals(toSend.getName(), toReceive.getName());
        assertEquals(toSend.getType(), toReceive.getType());
    }

}
