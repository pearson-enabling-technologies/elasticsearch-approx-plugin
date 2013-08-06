/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.joda.TimeZoneRounding;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;

import com.pearson.entech.elasticsearch.search.facet.approx.datehistogram.DistinctDateHistogramFacet.ComparatorType;

/**
 * A date histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class DistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final TimeZoneRounding tzRounding;
    private final IndexNumericFieldData indexFieldData;
    final ComparatorType comparatorType;

    final ExtTLongObjectHashMap<DistinctCountPayload> payloads;
    private final int maxExactPerShard;

    public DistinctDateHistogramFacetExecutor(final IndexNumericFieldData indexFieldData, final TimeZoneRounding tzRounding,
            final ComparatorType comparatorType, final int maxExactPerShard) {
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        this.tzRounding = tzRounding;
        this.maxExactPerShard = maxExactPerShard;
        this.payloads = new ExtTLongObjectHashMap<DistinctCountPayload>();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(final String facetName) {
        return new InternalDistinctCountDateHistogramFacet(facetName, comparatorType, payloads, true);
    }

    class Collector extends FacetExecutor.Collector {

        private LongValues values;
        private final DateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new DateHistogramProc(payloads, tzRounding);
        }

        @Override
        public void setNextReader(final AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getLongValues();
        }

        @Override
        public void collect(final int doc) throws IOException {
            histoProc.onDoc(doc, values);
        }

        @Override
        public void postCollection() {}
    }

    private void addItem(final long timestamp, final Object item) {
        if(payloads.containsKey(timestamp))
            payloads.get(timestamp).update(item);
        else
            payloads.put(timestamp, new DistinctCountPayload(this.maxExactPerShard).update(item));
    }

    public static class DateHistogramProc extends LongFacetAggregatorBase {

        private final ExtTLongObjectHashMap<DistinctCountPayload> payloads;
        private final TimeZoneRounding tzRounding;

        private long currTimestamp;

        public DateHistogramProc(final ExtTLongObjectHashMap<DistinctCountPayload> payloads, final TimeZoneRounding tzRounding) {
            this.payloads = payloads;
            this.tzRounding = tzRounding;
        }

        @Override
        public void onValue(final int docId, final long timestamp) {
            currTimestamp = tzRounding.calc(timestamp);
            addItem(currTimestamp, value);
        }

        public ExtTLongObjectHashMap<DistinctCountPayload> counts() {
            return payloads;
        }
    }
}