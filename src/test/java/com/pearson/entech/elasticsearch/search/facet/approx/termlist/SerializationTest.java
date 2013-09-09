package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

import static com.pearson.entech.elasticsearch.search.facet.approx.termlist.TestUtils.generateRandomWords;
import static org.junit.Assert.assertEquals;

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
    public void testStringSerialization() throws Exception {
        final List<String> randomWords = generateRandomWords(1000);
        final BytesRefHash sentHash = new BytesRefHash();
        for(final String word : randomWords) {
            sentHash.add(new BytesRef(word));
        }

        final InternalStringTermListFacet sentFacet = new InternalStringTermListFacet("foo", sentHash);
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
