package com.pearson.entech.elasticsearch.search.facet.approx.date;

import static com.google.common.collect.Maps.newHashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.elasticsearch.common.trove.map.hash.TObjectIntHashMap;
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
        final InternalCountingFacet toSend = new InternalCountingFacet("foo", sentCounts);
        final InternalCountingFacet toReceive = new InternalCountingFacet();
        serializeAndDeserialize(toSend, toReceive);
        final TLongIntHashMap receivedCounts = new TLongIntHashMap(toReceive.peekCounts());
        // Check against original counts as sentCounts may have been recycled
        compareCounts(counts, receivedCounts);
    }

    private void compareCounts(final TLongIntHashMap sentCounts, final TLongIntHashMap receivedCounts) {
        assertEquals(sentCounts.size(), receivedCounts.size());
        for(final long key : sentCounts.keys()) {
            assertTrue(receivedCounts.containsKey(key));
            assertEquals(sentCounts.get(key), receivedCounts.get(key));
        }
    }

    @Test
    public void testSerializingEmptyDistinctFacet() throws Exception {
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        final Map<Long, Integer> expectedCounts = newHashMap();
        testSerializingDistinctFacet(counts, expectedCounts, expectedCounts);
    }

    @Test
    public void testSerializingNonEmptyExactDistinctFacet() throws Exception {
        testSerializingNonEmptyDistinctFacet(999, 999);
    }

    @Test
    public void testSerializingNonEmptyApproxDistinctFacet() throws Exception {
        testSerializingNonEmptyDistinctFacet(0, 0);
    }

    @Test
    public void testSerializingNonEmptyMixedDistinctFacet() throws Exception {
        testSerializingNonEmptyDistinctFacet(0, 999);
    }

    private void testSerializingNonEmptyDistinctFacet(final int threshold1, final int threshold2) throws Exception {
        final DistinctCountPayload payload1 = new DistinctCountPayload(threshold1);
        payload1.update(new BytesRef("marge"));
        payload1.update(new BytesRef("homer"));
        final DistinctCountPayload payload2 = new DistinctCountPayload(threshold2);
        payload2.update(new BytesRef("bart"));
        payload2.update(new BytesRef("lisa"));
        final ExtTLongObjectHashMap<DistinctCountPayload> counts = CacheRecycler.popLongObjectMap();
        counts.put(1, payload1);
        counts.put(2, payload2);
        final Map<Long, Integer> expectedCounts = newHashMap();
        expectedCounts.put(1l, 2);
        expectedCounts.put(2l, 2);
        final Map<Long, Integer> expectedCardinalities = expectedCounts; // They're the same in this case
        testSerializingDistinctFacet(counts, expectedCounts, expectedCardinalities);
    }

    private void testSerializingDistinctFacet(final ExtTLongObjectHashMap<DistinctCountPayload> counts,
            final Map<Long, Integer> expectedCounts, final Map<Long, Integer> expectedCardinalities) throws Exception {
        final InternalDistinctFacet toSend = new InternalDistinctFacet("bar", counts);
        final InternalDistinctFacet toReceive = new InternalDistinctFacet();
        serializeAndDeserialize(toSend, toReceive);
        final ExtTLongObjectHashMap<DistinctCountPayload> receivedCounts =
                new ExtTLongObjectHashMap<DistinctCountPayload>(toReceive.peekCounts());
        compareDistinctCounts(expectedCounts, expectedCardinalities, receivedCounts);
    }

    private void compareDistinctCounts(final Map<Long, Integer> expectedCounts,
            final Map<Long, Integer> expectedCardinalities, final ExtTLongObjectHashMap<DistinctCountPayload> receivedCounts) {
        assertEquals(expectedCounts.size(), receivedCounts.size());
        for(final long period : expectedCounts.keySet()) {
            final DistinctCountPayload payload = receivedCounts.get(period);
            assertEquals(expectedCounts.get(period).intValue(), payload.getCount());
            assertEquals(expectedCardinalities.get(period).intValue(), payload.getCardinality().cardinality());
        }
    }

    @Test
    public void testSerializingEmptySlicedDistinctFacet() throws Exception {
        final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts = CacheRecycler.popLongObjectMap();
        final Map<Long, Map<BytesRef, Integer>> emptyMap = newHashMap();
        testSerializingSlicedDistinctFacet(counts, emptyMap, emptyMap);
    }

    @Test
    public void testSerializingNonEmptyExactSlicedDistinctFacet() throws Exception {
        testSerializingNonEmptySlicedDistinctFacet(999, 999);
    }

    @Test
    public void testSerializingNonEmptyApproxSlicedDistinctFacet() throws Exception {
        testSerializingNonEmptySlicedDistinctFacet(0, 0);
    }

    @Test
    public void testSerializingNonEmptyMixedSlicedDistinctFacet() throws Exception {
        testSerializingNonEmptySlicedDistinctFacet(0, 999);
    }

    private void testSerializingNonEmptySlicedDistinctFacet(final int threshold1, final int threshold2) throws Exception {
        final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts = CacheRecycler.popLongObjectMap();
        final BytesRef label1 = new BytesRef("itchy");
        final BytesRef label2 = new BytesRef("scratchy");
        final ExtTHashMap<BytesRef, DistinctCountPayload> period1 = CacheRecycler.popHashMap();
        final DistinctCountPayload payload1 = new DistinctCountPayload(threshold1);
        payload1.update(new BytesRef("marge"));
        payload1.update(new BytesRef("homer"));
        final DistinctCountPayload payload2 = new DistinctCountPayload(threshold1);
        payload2.update(new BytesRef("marge"));
        payload2.update(new BytesRef("marge"));
        period1.put(label1, payload1);
        period1.put(label2, payload2);
        counts.put(1, period1);
        final ExtTHashMap<BytesRef, DistinctCountPayload> period2 = CacheRecycler.popHashMap();
        final DistinctCountPayload payload3 = new DistinctCountPayload(threshold2);
        payload3.update(new BytesRef("bart"));
        payload3.update(new BytesRef("lisa"));
        final DistinctCountPayload payload4 = new DistinctCountPayload(threshold2);
        payload4.update(new BytesRef("bart"));
        payload4.update(new BytesRef("bart"));
        period2.put(label1, payload3);
        period2.put(label2, payload4);
        counts.put(2, period2);
        final Map<Long, Map<BytesRef, Integer>> expectedCounts = newHashMap();
        final Map<BytesRef, Integer> period1Counts = newHashMap();
        period1Counts.put(new BytesRef("itchy"), 2);
        period1Counts.put(new BytesRef("scratchy"), 2);
        expectedCounts.put(1l, period1Counts);
        final Map<BytesRef, Integer> period2Counts = newHashMap();
        period2Counts.put(new BytesRef("itchy"), 2);
        period2Counts.put(new BytesRef("scratchy"), 2);
        expectedCounts.put(2l, period2Counts);
        final Map<Long, Map<BytesRef, Integer>> expectedCards = newHashMap();
        final Map<BytesRef, Integer> period1Cards = newHashMap();
        period1Cards.put(new BytesRef("itchy"), 2);
        period1Cards.put(new BytesRef("scratchy"), 1);
        expectedCards.put(1l, period1Cards);
        final Map<BytesRef, Integer> period2Cards = newHashMap();
        period2Cards.put(new BytesRef("itchy"), 2);
        period2Cards.put(new BytesRef("scratchy"), 1);
        expectedCards.put(2l, period2Cards);
        testSerializingSlicedDistinctFacet(counts, expectedCounts, expectedCards);
    }

    private void testSerializingSlicedDistinctFacet(final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts,
            final Map<Long, Map<BytesRef, Integer>> expectedCounts, final Map<Long, Map<BytesRef, Integer>> expectedCards) throws Exception {
        final InternalSlicedDistinctFacet toSend = new InternalSlicedDistinctFacet("baz", counts);
        final InternalSlicedDistinctFacet toReceive = new InternalSlicedDistinctFacet();
        serializeAndDeserialize(toSend, toReceive);
        final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> receivedCounts =
                new ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>>(toReceive.peekCounts());
        // Check against original counts as sentCounts may have been recycled
        compareSlicedDistinctCounts(expectedCounts, expectedCards, receivedCounts);
    }

    private void compareSlicedDistinctCounts(final Map<Long, Map<BytesRef, Integer>> expectedCounts,
            final Map<Long, Map<BytesRef, Integer>> expectedCards, final ExtTLongObjectHashMap<ExtTHashMap<BytesRef, DistinctCountPayload>> counts) {
        assertEquals(expectedCounts.size(), counts.size());
        for(final long period : expectedCounts.keySet()) {
            assertTrue(counts.containsKey(period));
            final ExtTHashMap<BytesRef, DistinctCountPayload> receivedPeriod = counts.get(period);
            final Map<BytesRef, Integer> expectedPeriodCounts = expectedCounts.get(period);
            final Map<BytesRef, Integer> expectedPeriodCards = expectedCards.get(period);
            for(final BytesRef label : expectedCounts.get(period).keySet()) {
                assertTrue(receivedPeriod.containsKey(label));
                assertEquals(expectedPeriodCounts.get(label).intValue(), receivedPeriod.get(label).getCount());
                assertEquals(expectedPeriodCards.get(label).intValue(), receivedPeriod.get(label).getCardinality().cardinality());
            }
        }
    }

    @Test
    public void testSerializingEmptySlicedFacet() throws Exception {
        final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> counts = CacheRecycler.popLongObjectMap();
        testSerializingSlicedFacet(counts);
    }

    @Test
    public void testSerializingNonEmptySlicedFacet() throws Exception {
        final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> counts = CacheRecycler.popLongObjectMap();
        final BytesRef label1 = new BytesRef("itchy");
        final BytesRef label2 = new BytesRef("scratchy");
        final TObjectIntHashMap<BytesRef> period1 = CacheRecycler.popObjectIntMap();
        period1.put(label1, 1);
        period1.put(label2, 2);
        counts.put(1, period1);
        final TObjectIntHashMap<BytesRef> period2 = CacheRecycler.popObjectIntMap();
        period2.put(label1, 3);
        period2.put(label1, 4);
        counts.put(2, period2);
        testSerializingSlicedFacet(counts);
    }

    private void testSerializingSlicedFacet(final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> counts) throws Exception {
        final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> sentCounts =
                deepCopySliced(counts);
        final InternalSlicedFacet toSend = new InternalSlicedFacet("qux", sentCounts);
        final InternalSlicedFacet toReceive = new InternalSlicedFacet();
        serializeAndDeserialize(toSend, toReceive);
        final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> receivedCounts =
                new ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>>(toReceive.peekCounts());
        // Check against original counts as sentCounts may have been recycled
        compareSlicedCounts(counts, receivedCounts);
    }

    private ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> deepCopySliced(final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> counts) {
        final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> output =
                new ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>>();
        for(final long key : counts.keys()) {
            output.put(key, new TObjectIntHashMap<BytesRef>());
            for(final BytesRef br : counts.get(key).keySet()) {
                output.get(key).put(BytesRef.deepCopyOf(br), counts.get(key).get(br));
            }
        }
        return output;
    }

    private void compareSlicedCounts(final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> sentCounts,
            final ExtTLongObjectHashMap<TObjectIntHashMap<BytesRef>> receivedCounts) {
        assertEquals(sentCounts.size(), receivedCounts.size());
        for(final long key : sentCounts.keys()) {
            assertTrue(receivedCounts.containsKey(key));
            final TObjectIntHashMap<BytesRef> sentSlice = sentCounts.get(key);
            final TObjectIntHashMap<BytesRef> receivedSlice = receivedCounts.get(key);
            assertEquals(sentSlice.size(), receivedSlice.size());
            for(final BytesRef label : sentSlice.keySet()) {
                assertTrue(receivedSlice.containsKey(label));
                final int sentCount = sentSlice.get(label);
                final int receivedCount = receivedSlice.get(label);
                assertEquals(sentCount, receivedCount);
            }
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
        assertEquals(toSend.getName(), toReceive.getName());
        assertEquals(toSend.getType(), toReceive.getType());
    }

}
