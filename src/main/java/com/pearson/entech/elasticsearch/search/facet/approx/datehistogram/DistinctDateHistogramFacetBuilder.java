package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.AbstractFacetBuilder;

/**
 * A facet builder of approximate date histogram facets.
 */
public class DistinctDateHistogramFacetBuilder extends AbstractFacetBuilder {
    private String keyFieldName;
    private String valueFieldName;
    private String interval = null;
    private String preZone = null;
    private String postZone = null;
    private Boolean preZoneAdjustLargeInterval;
    private int maxExactPerShard = 0;
    long preOffset = 0;
    long postOffset = 0;
    float factor = 1.0f;
    private DistinctDateHistogramFacet.ComparatorType comparatorType;

    //private String valueScript;
    private Map<String, Object> params;

    //private String lang;

    /**
     * Constructs a new date histogram facet with the provided facet logical name.
     *
     * @param name The logical name of the facet
     */
    public DistinctDateHistogramFacetBuilder(final String name) {
        super(name);
    }

    /**
     * The field name to perform the histogram facet. Translates to perform the histogram facet
     * using the provided field as both the {@link #keyField(String)} and {@link #valueField(String)}.
     */
    public DistinctDateHistogramFacetBuilder field(final String field) {
        this.keyFieldName = field;
        return this;
    }

    /**
     * The field name to use in order to control where the hit will "fall into" within the histogram
     * entries. Essentially, using the key field numeric value, the hit will be "rounded" into the relevant
     * bucket controlled by the interval.
     */
    public DistinctDateHistogramFacetBuilder keyField(final String keyField) {
        this.keyFieldName = keyField;
        return this;
    }

    /**
     * The field name to use as the value of the hit to compute counts based on values within the interval.
     */
    public DistinctDateHistogramFacetBuilder valueField(final String valueField) {
        this.valueFieldName = valueField;
        return this;
    }

    public DistinctDateHistogramFacetBuilder param(final String name, final Object value) {
        if(params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return this;
    }

    /**
     * The interval used to control the bucket "size" where each key value of a hit will fall into. Check
     * the docs for all available values.
     */
    public DistinctDateHistogramFacetBuilder interval(final String interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Should pre zone be adjusted for large (day and above) intervals. Defaults to <tt>false</tt>.
     */
    public DistinctDateHistogramFacetBuilder preZoneAdjustLargeInterval(final boolean preZoneAdjustLargeInterval) {
        this.preZoneAdjustLargeInterval = preZoneAdjustLargeInterval;
        return this;
    }

    /**
     * Sets the pre time zone to use when bucketing the values. This timezone will be applied before
     * rounding off the result.
     * <p/>
     * Can either be in the form of "-10:00" or
     * one of the values listed here: http://joda-time.sourceforge.net/timezones.html.
     */
    public DistinctDateHistogramFacetBuilder preZone(final String preZone) {
        this.preZone = preZone;
        return this;
    }

    /**
     * Sets the post time zone to use when bucketing the values. This timezone will be applied after
     * rounding off the result.
     * <p/>
     * Can either be in the form of "-10:00" or
     * one of the values listed here: http://joda-time.sourceforge.net/timezones.html.
     */
    public DistinctDateHistogramFacetBuilder postZone(final String postZone) {
        this.postZone = postZone;
        return this;
    }

    /**
     * Sets a pre offset that will be applied before rounding the results.
     */
    public DistinctDateHistogramFacetBuilder preOffset(final TimeValue preOffset) {
        this.preOffset = preOffset.millis();
        return this;
    }

    /**
     * Sets a post offset that will be applied after rounding the results.
     */
    public DistinctDateHistogramFacetBuilder postOffset(final TimeValue postOffset) {
        this.postOffset = postOffset.millis();
        return this;
    }

    /**
     * Sets the factor that will be used to multiply the value with before and divided
     * by after the rounding of the results.
     */
    public DistinctDateHistogramFacetBuilder factor(final float factor) {
        this.factor = factor;
        return this;
    }

    /**
     * Sets the number of exact values that are allowed per shard before we fall back to doing
     * approximate counts.
     */
    public DistinctDateHistogramFacetBuilder maxExactPerShard(final int maxExactPerShard) {
        this.maxExactPerShard = maxExactPerShard;
        return this;
    }

    public DistinctDateHistogramFacetBuilder comparator(final DistinctDateHistogramFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        return this;
    }

    /**
     * Marks the facet to run in a specific scope.
     */
    @Override
    public DistinctDateHistogramFacetBuilder scope(final String scope) {
        super.scope(scope);
        return this;
    }

    /**
     * An additional filter used to further filter down the set of documents the facet will run on.
     */
    @Override
    public DistinctDateHistogramFacetBuilder facetFilter(final FilterBuilder filter) {
        this.facetFilter = filter;
        return this;
    }

    /**
     * Sets the nested path the facet will execute on. A match (root object) will then cause all the
     * nested objects matching the path to be computed into the facet.
     */
    @Override
    public DistinctDateHistogramFacetBuilder nested(final String nested) {
        this.nested = nested;
        return this;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        if(keyFieldName == null) {
            throw new SearchSourceBuilderException("field must be set on date histogram facet for facet [" + name + "]");
        }
        if(interval == null) {
            throw new SearchSourceBuilderException("interval must be set on date histogram facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject(DistinctDateHistogramFacet.TYPE);
        if(valueFieldName != null) {
            builder.field("key_field", keyFieldName);
            builder.field("value_field", valueFieldName);
        } else {
            builder.field("field", keyFieldName);
        }
        builder.field("interval", interval);
        if(preZone != null) {
            builder.field("pre_zone", preZone);
        }
        if(preZoneAdjustLargeInterval != null) {
            builder.field("pre_zone_adjust_large_interval", preZoneAdjustLargeInterval);
        }
        if(postZone != null) {
            builder.field("post_zone", postZone);
        }
        if(preOffset != 0) {
            builder.field("pre_offset", preOffset);
        }
        if(postOffset != 0) {
            builder.field("post_offset", postOffset);
        }
        if(factor != 1.0f) {
            builder.field("factor", factor);
        }
        if(maxExactPerShard != 0) {
            builder.field("max_exact_per_shard", maxExactPerShard);
        }
        if(comparatorType != null) {
            builder.field("comparator", comparatorType.description());
        }
        builder.endObject();

        addFilterFacetAndGlobal(builder, params);

        builder.endObject();
        return builder;
    }

}
