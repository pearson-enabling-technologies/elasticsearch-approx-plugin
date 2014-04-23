package com.pearson.entech.elasticsearch.search.facet.approx.termlist;

/**
 * Some constants used by term list.
 */
public interface Constants {

    /**
     * Default max terms returned per shard.
     */
    int DEFAULT_MAX_PER_SHARD = 1000;

    /**
     * Default collector-mode sampling ratio (1 = no sampling, use all docs).
     */
    float DEFAULT_SAMPLE = 1;

    /**
     * Supported modes.
     */
    enum MODE {

        /**
         * Collector mode -- iterate over docs, possibly with sampling.
         */
        COLLECTOR,

        /**
         * Post mode -- iterate over terms that occur in matched docs.
         */
        POST
    }

    /**
     * High-level data type of the field data for a field.
     */
    enum FIELD_DATA_TYPE {

        /**
         * Unknown or couldn't be determined.
         */
        UNDEFINED,

        /**
         * 32-bit integer.
         */
        INT,

        /**
         * 64-bit long integer.
         */
        LONG,

        /**
         * String.
         */
        STRING
    }

    /**
     * Human-readable name of collector mode.
     */
    String COLLECTOR_MODE = "collector";

    /**
     * Human-readable name of post mode.
     */
    String POST_MODE = "post";

}
