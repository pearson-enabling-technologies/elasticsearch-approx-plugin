package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.map.hash.TLongObjectHashMap;
import org.elasticsearch.search.facet.InternalFacet;
import org.junit.Test;

public class SerializationTest {

    @Test
    public void testSerializingEmptyCountingFacet() throws Exception {
        final TLongIntHashMap counts = CacheRecycler.popLongIntMap();
        testSerializingCountingFacet(counts);
    }

    @Test
    public void testSerializingNonEmptyCountingFacet() throws Exception {
        final TLongIntHashMap counts = CacheRecycler.popLongIntMap();
        counts.put(1, 2);
        counts.put(11, 22);
        testSerializingCountingFacet(counts);
    }

    private void testSerializingCountingFacet(final TLongIntHashMap counts) throws Exception {
        final TLongIntHashMap sentCounts = new TLongIntHashMap(counts);
        final InternalCountingFacet toSend = new InternalCountingFacet("foo", counts);
        final InternalCountingFacet toReceive = new InternalCountingFacet();
        serializeAndDeserialize(toSend, toReceive);
        assertEquals(toSend.getName(), toReceive.getName());
        assertEquals(toSend.getType(), toReceive.getType());
        final TLongIntHashMap receivedCounts = new TLongIntHashMap(toReceive.peekCounts());
        compare(sentCounts, receivedCounts);
    }

    private void compare(final TLongIntHashMap sentCounts, final TLongIntHashMap receivedCounts) {
        assertEquals(sentCounts.size(), receivedCounts.size());
        for(final long key : sentCounts.keys()) {
            assertTrue(receivedCounts.containsKey(key));
            assertEquals(sentCounts.get(key), receivedCounts.get(key));
        }
    }

    @Test
    public void testSerializingEmptyDistinctFacet() throws Exception {
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        testSerializingDistinctFacet(counts);
    }

    @Test
    public void testSerializingNonEmptyExactDistinctFacet() throws Exception {
        final DistinctCountPayload payload1 = new DistinctCountPayload(99999);
        payload1.update(new BytesRef("marge"));
        payload1.update(new BytesRef("homer"));
        final DistinctCountPayload payload2 = new DistinctCountPayload(99999);
        payload2.update(new BytesRef("bart"));
        payload2.update(new BytesRef("lisa"));
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        testSerializingDistinctFacet(counts);
    }

    @Test
    public void testSerializingNonEmptyApproxDistinctFacet() throws Exception {
        final DistinctCountPayload payload1 = new DistinctCountPayload(0);
        payload1.update(new BytesRef("marge"));
        payload1.update(new BytesRef("homer"));
        final DistinctCountPayload payload2 = new DistinctCountPayload(0);
        payload2.update(new BytesRef("bart"));
        payload2.update(new BytesRef("lisa"));
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        testSerializingDistinctFacet(counts);
    }

    @Test
    public void testSerializingNonEmptyMixedDistinctFacet() throws Exception {
        final DistinctCountPayload payload1 = new DistinctCountPayload(0);
        payload1.update(new BytesRef("marge"));
        payload1.update(new BytesRef("homer"));
        final DistinctCountPayload payload2 = new DistinctCountPayload(99999);
        payload2.update(new BytesRef("bart"));
        payload2.update(new BytesRef("lisa"));
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        testSerializingDistinctFacet(counts);
    }

    private void testSerializingDistinctFacet(final TLongObjectHashMap<DistinctCountPayload> counts) throws Exception {
        final ExtTLongObjectHashMap<DistinctCountPayload> sentCounts = new ExtTLongObjectHashMap<DistinctCountPayload>(counts);
        final InternalDistinctFacet toSend = new InternalDistinctFacet("bar", sentCounts);
        final InternalDistinctFacet toReceive = new InternalDistinctFacet();
        serializeAndDeserialize(toSend, toReceive);
        assertEquals(toSend.getName(), toReceive.getName());
        assertEquals(toSend.getType(), toReceive.getType());
        final ExtTLongObjectHashMap<DistinctCountPayload> receivedCounts = new ExtTLongObjectHashMap(toReceive.peekCounts());
        compare(sentCounts, receivedCounts);
    }

    private void compare(final ExtTLongObjectHashMap<DistinctCountPayload> sentCounts, final ExtTLongObjectHashMap<DistinctCountPayload> receivedCounts) {
        assertEquals(sentCounts.size(), receivedCounts.size());
        for(final long key : sentCounts.keys()) {
            assertTrue(receivedCounts.containsKey(key));
            final DistinctCountPayload sentVal = sentCounts.get(key);
            final DistinctCountPayload receivedVal = receivedCounts.get(key);
            assertEquals(sentVal.getCount(), receivedVal.getCount());
            assertEquals(sentVal.getCardinality().cardinality(), receivedVal.getCardinality().cardinality());
        }
    }

    private <T extends InternalFacet> void serializeAndDeserialize(final T toSend, final T toReceive) throws Exception {
        final BytesStreamOutput bso = new BytesStreamOutput();
        toSend.writeTo(bso);
        bso.close();
        final BytesReference bytes = bso.bytes();
        final BytesStreamInput bsi = new BytesStreamInput(bytes);
        toReceive.readFrom(bsi);
        bsi.close();
    }

}
