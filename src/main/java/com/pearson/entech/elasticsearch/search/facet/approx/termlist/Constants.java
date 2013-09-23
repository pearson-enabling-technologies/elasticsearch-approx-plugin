package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

public interface Constants {

    int DEFAULT_MAX_PER_SHARD = 1000;

    float DEFAULT_SAMPLE = 1;

    enum MODE {
        COLLECTOR,
        POST
    }

    enum FIELD_DATA_TYPE {
        UNDEFINED,
        INT,
        LONG,
        STRING
    }

    String COLLECTOR_MODE = "collector";
    String POST_MODE = "post";

}
