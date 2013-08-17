package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import org.elasticsearch.common.xcontent.XContentBuilderString;

public interface Constants {

    static final XContentBuilderString _TYPE = new XContentBuilderString("_type");

    static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");

    static final XContentBuilderString TIME = new XContentBuilderString("time");

    static final XContentBuilderString SLICES = new XContentBuilderString("slices");

    static final XContentBuilderString LABELS = new XContentBuilderString("labels");

    static final XContentBuilderString COUNT = new XContentBuilderString("count");

    static final XContentBuilderString DISTINCT_COUNT = new XContentBuilderString("distinct_count");

    static final XContentBuilderString DISTINCT_FIELD = new XContentBuilderString("distinct_field");

    static final XContentBuilderString SLICE_FIELD = new XContentBuilderString("slice_field");

    static final XContentBuilderString TERM = new XContentBuilderString("term");

}
