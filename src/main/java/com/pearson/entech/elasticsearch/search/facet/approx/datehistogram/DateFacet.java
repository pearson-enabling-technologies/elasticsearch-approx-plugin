package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.InternalFacet;

public abstract class DateFacet<P extends ToXContent> extends InternalFacet {

    private long _totalCount;

    public DateFacet(final String name) {
        super(name);
    }

    public long getTotalCount() {
        return _totalCount;
    }

    protected void setTotalCount(final long count) {
        _totalCount = count;
    }

    public abstract List<P> getTimePeriods();

    public List<P> getEntries() {
        return getTimePeriods();
    }

    public List<P> entries() {
        return getTimePeriods();
    }

    /**
     * Just for testing -- spy on current internal state of _counts.
     * 
     * @param <T> the expected type of this implementation's _counts field
     * @return the current value of _counts
     */
    abstract <T> T peekCounts();

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Constants._TYPE, getType());
        builder.field(Constants.COUNT, getTotalCount());
        injectHeaderXContent(builder);
        builder.startArray(Constants.ENTRIES);
        for(final P period : getTimePeriods()) {
            period.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public final void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        readData(in);
    }

    @Override
    public final void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        writeData(out);
        releaseCache();
    }

    protected abstract void releaseCache();

    protected abstract void writeData(StreamOutput out) throws IOException;

    protected abstract void readData(StreamInput oIn) throws IOException;

    protected void injectHeaderXContent(final XContentBuilder builder) throws IOException {
        // override to add extra top-level fields, before the entries list
    }

}
