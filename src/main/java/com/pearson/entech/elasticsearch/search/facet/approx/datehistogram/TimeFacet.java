package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.InternalFacet;

public abstract class TimeFacet<P extends ToXContent> extends InternalFacet {

    private long _totalCount;

    public TimeFacet(final String name) {
        super(name);
    }

    public long getTotalCount() {
        return _totalCount;
    }

    protected void setTotalCount(final long count) {
        _totalCount = count;
    }

    public abstract List<P> getTimePeriods();

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
        final ObjectInputStream oIn = new ObjectInputStream(in);
        try {
            readData(oIn);
        } catch(final ClassNotFoundException e) {
            throw new IOException("Deserialization of " + getName() + " failed", e);
        }
    }

    @Override
    public final void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        final ObjectOutputStream oOut = new ObjectOutputStream(out);
        writeData(oOut);
        releaseCache();
    }

    protected abstract void releaseCache();

    protected abstract void writeData(ObjectOutputStream oOut) throws IOException;

    protected abstract void readData(ObjectInputStream oIn) throws ClassNotFoundException, IOException;

    protected void injectHeaderXContent(final XContentBuilder builder) {
        // override to add extra top-level fields, before the entries list
    }

}
