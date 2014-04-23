package com.pearson.entech.elasticsearch.search.facet.approx.date.internal;

import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.map.TLongLongMap;
import org.elasticsearch.common.trove.map.hash.TLongLongHashMap;
import org.elasticsearch.common.trove.procedure.TLongLongProcedure;
import org.elasticsearch.search.facet.Facet;

import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.NullEntry;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.TimePeriod;

/**
 * The internal representation of a standard DateFacet, holding the reduce and
 * materialize logic. This is only available on the server side, or via
 * ElasticSearch's JVM transport -- i.e. it doesn't survive XContent serialization.
 */
public class InternalCountingFacet extends DateFacet<TimePeriod<NullEntry>> {

    /**
     * The facet type, as shown in the JSON returned.
     */
    static final String TYPE = "counting_date_facet";

    /**
     * Stream handler for this facet type.
     */
    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            final InternalCountingFacet facet = new InternalCountingFacet();
            facet.readFrom(in);
            return facet;
        }
    };

    /**
     * Register the stream handler with ElasticSearch.
     */
    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    /**
     * The stream type, used internally by ElasticSearch.
     */
    private static final BytesReference STREAM_TYPE = new HashedBytesArray(TYPE.getBytes());

    /**
     * An empty counts map, shared between instances.
     */
    private static final TLongLongMap EMPTY = new TLongLongHashMap();

    /**
     * Map from timestamps to counts.
     */
    private TLongLongMap _counts;

    /**
     * Total count across all time periods.
     */
    private long _total;

    /**
     * List of time periods, only created in the materialize phase.
     */
    private List<TimePeriod<NullEntry>> _periods;

    /**
     * Empty constructor for deserialization only -- do not use.
     */
    protected InternalCountingFacet() {
        super("not set");
    }

    /**
     * Create a new facet from an existing counts map.
     * 
     * @param name the name of this facet as supplied by the user
     * @param counts the counts map
     */
    public InternalCountingFacet(final String name, final TLongLongMap counts) {
        super(name);
        _counts = counts;
    }

    @Override
    public long getTotalCount() {
        materialize();
        return _total;
    }

    @Override
    public List<TimePeriod<NullEntry>> getTimePeriods() {
        materialize();
        return _periods;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected TLongLongMap peekCounts() {
        return _counts;
    }

    @Override
    protected void readData(final StreamInput in) throws IOException {
        _counts = CacheRecycler.popLongLongMap();
        final int size = in.readVInt();
        for(int i = 0; i < size; i++) {
            final long key = in.readVLong();
            final long val = in.readVLong();
            _counts.put(key, val);
        }
    }

    @Override
    protected void writeData(final StreamOutput out) throws IOException {
        if(_counts == null) {
            out.writeVInt(0);
            return;
        }
        final int size = _counts.size();
        _serialize.init(out, size);
        _counts.forEachEntry(_serialize);
        _serialize.clear();
    }

    @Override
    public Facet reduce(final List<Facet> facets) {
        if(facets.size() > 0) {
            // Reduce into the first facet; we will release its _counts on materializing
            final InternalCountingFacet target = (InternalCountingFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalCountingFacet source = (InternalCountingFacet) facets.get(i);
                // For each datetime period in the new facet...
                _mergePeriods.target = target;
                source._counts.forEachEntry(_mergePeriods);
                _mergePeriods.target = null;
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalCountingFacet(getName(), EMPTY);
        }
    }

    /**
     * Prepare this facet for rendering, clearing any held data in the process.
     */
    private synchronized void materialize() {
        if(_periods != null)
            // This facet has been materialized already
            return;
        if(_counts == null || _counts.size() == 0) {
            _total = 0;
            _periods = newArrayListWithCapacity(0);
            return;
        }
        _periods = newArrayListWithCapacity(_counts.size());
        _materializePeriod.init(_periods);
        _counts.forEachEntry(_materializePeriod);
        _total = _materializePeriod.counter;
        _materializePeriod.clear();
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        releaseCache();
    }

    @Override
    protected void releaseCache() {
        CacheRecycler.pushLongLongMap((TLongLongHashMap) _counts);
    }

    private final PeriodMerger _mergePeriods = new PeriodMerger();

    /**
     * Performs merge operation over the elements of a counts map,
     * adding/incrementing the corresponding values in a target map.
     */
    private static final class PeriodMerger implements TLongLongProcedure {

        InternalCountingFacet target;

        @Override
        public boolean execute(final long time, final long count) {
            // Called once per time period:
            // increment the corresponding count in the target facet, or add if not there
            target._counts.adjustOrPutValue(time, count, count);
            return true;
        }

    }

    private final PeriodMaterializer _materializePeriod = new PeriodMaterializer();

    /**
     * Performs materialize operation over a counts map, converting
     * the counts into a list of TimePeriod objects. This list will
     * be in the same order as the map entries were provided.
     */
    private static final class PeriodMaterializer implements TLongLongProcedure {

        private List<TimePeriod<NullEntry>> _target;

        long counter;

        /**
         * Initialize or reinitialize the procedure, providing a list
         * to materialize the data into. The counter will be reset to 0.
         * 
         * @param periods the target list, should be empty
         */
        public void init(final List<TimePeriod<NullEntry>> periods) {
            _target = periods;
            counter = 0;
        }

        @Override
        public boolean execute(final long time, final long count) {
            // Called once per period:
            // create a TimePeriod representation of this count and save it
            _target.add(new TimePeriod<NullEntry>(time, count, NullEntry.INSTANCE));
            counter += count;
            return true;
        }

        /**
         * Release any resources held by the procedure so they can
         * be garbage-collected.
         */
        public void clear() {
            _target = null;
        }

    }

    private final Serializer _serialize = new Serializer();

    /**
     * Performs serialize operation over a counts map, writing
     * the counts into an ElasticSearch StreamOutput object.
     */
    private static final class Serializer implements TLongLongProcedure {

        private StreamOutput _output;

        /**
         * Initialize or reinitialize the procedure, providing a
         * StreamOutput to write the data to.
         * 
         * @param output the StreamOutput
         * @param size the number of counts to write
         * @throws IOException
         */
        public void init(final StreamOutput output, final int size) throws IOException {
            _output = output;
            output.writeVInt(size);
        }

        @Override
        public boolean execute(final long key, final long val) {
            // Called once per period:
            // write the timestamp and value to _output
            try {
                _output.writeVLong(key);
                _output.writeVLong(val);
            } catch(final IOException e) {
                throw new IllegalStateException(e);
            }
            return true;
        }

        /**
         * Release any resources held by the procedure so they can
         * be garbage-collected.
         */
        public void clear() {
            _output = null;
        }

    }

}
