package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.facet.InternalFacet;


/**
 * Top-level external-facing date facet class.
 * 
 * @param <P> the type of the period objects this facet contains
 */
public abstract class DateFacet<P extends ToXContent> extends InternalFacet {

    private long _totalCount;

    /**
     * Create a new date facet.
     * 
     * @param name the facet name
     */
    public DateFacet(final String name) {
        super(name);
    }

    /**
     * Get the total count reported by this facet. This will either be a count
     * of documents, or a count of occurrences of value_field, if value_field
     * was supplied.
     * 
     * @return the count
     */
    public long getTotalCount() {
        return _totalCount;
    }

    /**
     * Set the total count.
     * 
     * @param count the new count
     */
    protected void setTotalCount(final long count) {
        _totalCount = count;
    }

    /**
     * Get the time periods covered by this facet.
     * 
     * @return a list of time period objects
     */
    public abstract List<P> getTimePeriods();

    /**
     * Get the time periods covered by this facet.
     * 
     * @return a list of time period objects
     */
    public List<P> getEntries() {
        return getTimePeriods();
    }

    /**
     * Get the time periods covered by this facet.
     * 
     * @return a list of time period objects
     */
    public List<P> entries() {
        return getTimePeriods();
    }

    /**
     * Just for testing -- spy on current internal state of _counts.
     * 
     * @param <T> the expected type of this implementation's _counts field
     * @return the current value of _counts
     */
    protected abstract <T> T peekCounts();

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

    /**
     * Free up any data structures for garbage collection.
     */
    protected abstract void releaseCache();

    /**
     * Serialize the facet data to a stream.
     * 
     * @param out the StreamOutput
     * @throws IOException
     */
    protected abstract void writeData(StreamOutput out) throws IOException;

    /**
     * Deserialize the facet data from a stream.
     * 
     * @param in the StreamInput
     * @throws IOException
     */
    protected abstract void readData(StreamInput in) throws IOException;

    /**
     * Override this class to inject additional header fields before the list
     * of facet entries (time periods) -- e.g. distinct counts. This method
     * will be called at the appropriate time by DateFacet's own toXContent() method.
     * 
     * @param builder an XContentBuilder to use
     * @throws IOException
     */
    protected void injectHeaderXContent(final XContentBuilder builder) throws IOException {}

}
