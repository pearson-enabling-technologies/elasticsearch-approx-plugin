package com.pearson.entech.elasticsearch.search.facet.approx.date;

import org.elasticsearch.common.xcontent.XContentBuilderString;

/**
 * Various XContent constants to speed up rendering.
 */
public interface Constants {

    /** String representing the facet type field. */
    static final XContentBuilderString _TYPE = new XContentBuilderString("_type");

    /** String representing the facet entried field. */
    static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");

    /** String representing the timestamp field of a facet entry. */
    static final XContentBuilderString TIME = new XContentBuilderString("time");

    /** String representing the slices field within a sliced facet. */
    static final XContentBuilderString SLICES = new XContentBuilderString("slices");

    /** String representing the facet type field. */
    static final XContentBuilderString LABELS = new XContentBuilderString("labels");

    /** String representing the count field. */
    static final XContentBuilderString COUNT = new XContentBuilderString("count");

    /** String representing the distinct count field. */
    static final XContentBuilderString DISTINCT_COUNT = new XContentBuilderString("distinct_count");

    /** String representing the "distinct field" field. */
    static final XContentBuilderString DISTINCT_FIELD = new XContentBuilderString("distinct_field");

    /** String representing the "slice field" field. */
    static final XContentBuilderString SLICE_FIELD = new XContentBuilderString("slice_field");

    /** String representing the term field. */
    static final XContentBuilderString TERM = new XContentBuilderString("term");

}
