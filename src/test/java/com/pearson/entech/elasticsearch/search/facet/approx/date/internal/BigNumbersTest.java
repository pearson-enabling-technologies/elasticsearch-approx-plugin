package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.map.TLongLongMap;
import org.elasticsearch.common.trove.map.hash.TLongIntHashMap;
import org.junit.Test;

/**
 * User: Nikos Tzanos
 * Date: 28/04/2014
 * Time: 19:21
 */
public class BigNumbersTest {

    private final TLongLongMap map = CacheRecycler.popLongLongMap();
    private final TLongIntHashMap intMap = CacheRecycler.popLongIntMap();

    private TLongLongMap createBigNumber(final int elements) {


        long pow_of_two_long = 1;
        int pow_of_two_int = 1;

        for (int i = 0; i < elements; i++) {
            pow_of_two_long *= 2;
            pow_of_two_int *= 2;
        }

        map.adjustOrPutValue(1, pow_of_two_long, pow_of_two_long);

        map.adjustOrPutValue(2, 1, 1);
        map.adjustOrPutValue(3, 1, 1);

        intMap.adjustOrPutValue(1, pow_of_two_int, pow_of_two_int);

        intMap.adjustOrPutValue(2, 1, 1);
        intMap.adjustOrPutValue(3, 1, 1);

        return map;

    }

    @Test
    public void testBigNumbers() {
        InternalCountingFacet facet = new InternalCountingFacet("test", createBigNumber(32));
        System.out.println(map.get(0) + map.get(1) + map.get(2));

        System.out.println(facet.getTotalCount());

        System.out.println(intMap.get(0) + intMap.get(1) + intMap.get(2));
        System.out.println(String.format("intMap.get(0) : %s, intMap.get(1) : %s, intMap.get(2) : %s", intMap.get(0), intMap.get(1), intMap.get(2)));

    }
}
