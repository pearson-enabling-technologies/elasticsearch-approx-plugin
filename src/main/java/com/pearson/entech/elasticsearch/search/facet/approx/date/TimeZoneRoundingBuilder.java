package com.pearson.entech.elasticsearch.search.facet.approx.date;

import org.elasticsearch.common.joda.TimeZoneRounding.Builder;
import org.elasticsearch.common.joda.time.DateTimeField;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.unit.TimeValue;

public class TimeZoneRoundingBuilder extends org.elasticsearch.common.joda.TimeZoneRounding.Builder {

    private DateTimeField field;
    private final long interval = -1;

    private final DateTimeZone preTz = DateTimeZone.UTC;
    private final DateTimeZone postTz = DateTimeZone.UTC;

    private final float factor = 1.0f;

    private long preOffset;
    private long postOffset;

    StringBuilder _descriptor;

    private final boolean preZoneAdjustLargeInterval = false;

    public TimeZoneRoundingBuilder(final DateTimeField field) {
        super(field);
        _descriptor = new StringBuilder("TimeZoneRounding ")
                .append(field);
    }

    public TimeZoneRoundingBuilder(final TimeValue interval) {
        super(interval);
        _descriptor = new StringBuilder("TimeZoneRounding ")
                .append("TimeValue[" + interval + "] ");
    }

    @Override
    public Builder preZone(final DateTimeZone preTz) {
        _descriptor.append(" preZone[")
                .append(preTz)
                .append("]");
        return super.preZone(preTz);
    }

    @Override
    public Builder postZone(final DateTimeZone postTz) {
        _descriptor.append(" postZone[")
                .append(postTz)
                .append("]");
        return super.postZone(postTz);
    }

    @Override
    public Builder preOffset(final long preOffset) {
        _descriptor.append(" preOffset[")
                .append(preOffset)
                .append("]");
        return super.preOffset(preOffset);
    }

    @Override
    public Builder postOffset(final long postOffset) {
        _descriptor.append(" postOffset[")
                .append(postOffset)
                .append("]");
        return super.postOffset(postOffset);
    }

    @Override
    public Builder factor(final float factor) {
        _descriptor.append(" factor[")
                .append(factor)
                .append("]");
        return super.factor(factor);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public boolean equals(final Object obj) {
        return super.equals(obj);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return _descriptor.toString();
    }

}
