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
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.trove.procedure.TLongObjectProcedure;
import org.elasticsearch.search.facet.Facet;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DistinctDateFacet;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.DistinctTimePeriod;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.HasDistinct;
import com.pearson.entech.elasticsearch.search.facet.approx.date.external.NullEntry;

/**
 * The internal representation of a DistinctDateFacet, holding the reduce and
 * materialize logic. This is only available on the server side, or via
 * ElasticSearch's JVM transport -- i.e. it doesn't survive XContent serialization.
 */
public class InternalDistinctFacet extends DistinctDateFacet<DistinctTimePeriod<NullEntry>> implements HasDistinct {

    /**
     * The facet type, as shown in the JSON returned.
     */
    static final String TYPE = "distinct_date_facet";

    /**
     * Stream handler for this facet type.
     */
    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(final StreamInput in) throws IOException {
            final InternalDistinctFacet facet = new InternalDistinctFacet();
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
     * An empty distinct counts map, shared between instances.
     */
    private static final ExtTLongObjectHashMap<DistinctCountPayload> EMPTY = new ExtTLongObjectHashMap<DistinctCountPayload>();

    /**
     * Map from timestamps to distinct counts.
     */
    private ExtTLongObjectHashMap<DistinctCountPayload> _counts;

    /**
     * Total count across all time periods.
     */
    private long _total;

    /**
     * Total distinct count across all time periods.
     */
    private long _distinctCount;

    /**
     * List of time periods, only created in the materialize phase.
     */
    private List<DistinctTimePeriod<NullEntry>> _periods;

    /**
     * Empty constructor for deserialization only -- do not use.
     */
    protected InternalDistinctFacet() {
        super("not set");
    }

    /**
     * Create a new facet from an existing distinct counts map.
     * 
     * @param name the name of this facet as supplied by the user
     * @param counts the distinct counts map
     */
    public InternalDistinctFacet(final String name,
            final ExtTLongObjectHashMap<DistinctCountPayload> counts) {
        super(name);
        _counts = counts;
    }

    @Override
    public long getTotalCount() {
        materialize();
        return _total;
    }

    @Override
    public long getDistinctCount() {
        materialize();
        return _distinctCount;
    }

    @Override
    public List<DistinctTimePeriod<NullEntry>> getTimePeriods() {
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
    protected ExtTLongObjectHashMap<DistinctCountPayload> peekCounts() {
        return _counts;
    }

    @Override
    protected void readData(final StreamInput in) throws IOException {
        _counts = CacheRecycler.popLongObjectMap();
        final int size = in.readVInt();
        for(int i = 0; i < size; i++) {
            final long key = in.readVLong();
            _counts.put(key, new DistinctCountPayload(in));
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
            // Reduce into the first facet; we will release its _counts on rendering into XContent
            final InternalDistinctFacet target = (InternalDistinctFacet) facets.get(0);
            for(int i = 1; i < facets.size(); i++) {
                final InternalDistinctFacet source = (InternalDistinctFacet) facets.get(i);
                _mergePeriods.target = target;
                source._counts.forEachEntry(_mergePeriods);
                _mergePeriods.target = null; // Avoid risk of garbage leaks
                // Release contents of source facet; no longer needed
                source.releaseCache();
            }
            return target;
        } else {
            return new InternalDistinctFacet(getName(), EMPTY);
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
            _distinctCount = 0;
            _periods = newArrayListWithCapacity(0);
            return;
        }
        _periods = newArrayListWithCapacity(_counts.size());
        _materializePeriod.init(_periods);
        _counts.forEachEntry(_materializePeriod);
        Collections.sort(_periods, ChronologicalOrder.INSTANCE);
        _total = _materializePeriod.getOverallTotal();
        _distinctCount = _materializePeriod.getOverallDistinct();
        _materializePeriod.clear();
        releaseCache();
    }

    @Override
    protected void releaseCache() {
        CacheRecycler.pushLongObjectMap(_counts);
    }

    private final PeriodMerger _mergePeriods = new PeriodMerger();

    /**
     * Performs merge operation over the elements of a distinct counts map,
     * updating the corresponding payloads in a target map.
     */
    private static class PeriodMerger implements TLongObjectProcedure<DistinctCountPayload> {

        InternalDistinctFacet target;

        @Override
        public boolean execute(final long time, final DistinctCountPayload payload) {
            // Called once per period:
            // update the corresponding payload in the target facet, or add if not there
            payload.mergeInto(target._counts, time);
            return true;
        }

    }

    private final PeriodMaterializer _materializePeriod = new PeriodMaterializer();

    /**
     * Performs materialize operation over a distinct counts map, converting
     * the counts into a list of TimePeriod objects. This list will
     * be in the same order as the map entries were provided.
     */
    private static final class PeriodMaterializer implements TLongObjectProcedure<DistinctCountPayload> {

        private List<DistinctTimePeriod<NullEntry>> _target;

        private DistinctCountPayload _accumulator;

        /**
         * Initialize or reinitialize the procedure, providing a list
         * to materialize the data into. The counter will be reset to 0.
         * 
         * @param periods the target list, should be empty
         */
        public void init(final List<DistinctTimePeriod<NullEntry>> periods) {
            _target = periods;
            _accumulator = null;
        }

        /**
         * Get the total (non-distinct) count across all time periods.
         * 
         * @return the count
         */
        public long getOverallTotal() {
            return _accumulator == null ?
                    0 : _accumulator.getCount();
        }

        /**
         * Get the distinct count across all time periods.
         * 
         * @return the count
         */
        public long getOverallDistinct() {
            return _accumulator == null ?
                    0 : _accumulator.getDistinctCount();
        }

        @Override
        public boolean execute(final long time, final DistinctCountPayload payload) {
            // Called once per period:
            // create a TimePeriod representation of this count and save it
            final long count = payload.getCount();
            final long cardinality = payload.getDistinctCount();
            _target.add(new DistinctTimePeriod<NullEntry>(
                    time, count, cardinality, NullEntry.INSTANCE));

            // Save the first payload we receive, and merge the others into it
            if(_accumulator == null)
                _accumulator = payload;
            else
                try {
                    _accumulator.merge(payload);
                } catch(final CardinalityMergeException e) {
                    throw new IllegalStateException(e);
                }

            return true;
        }

        /**
         * Release any resources held by the procedure so they can
         * be garbage-collected.
         */
        public void clear() {
            _target = null;
            _accumulator = null;
        }

    }

    private final Serializer _serialize = new Serializer();

    /**
     * Performs serialize operation over a distinct counts map, writing
     * the counts into an ElasticSearch StreamOutput object.
     */
    private static final class Serializer implements TLongObjectProcedure<DistinctCountPayload> {

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
        public boolean execute(final long key, final DistinctCountPayload payload) {
            // Called once per period:
            // write the timestamp and value to _output
            try {
                _output.writeVLong(key);
                payload.writeTo(_output);
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
