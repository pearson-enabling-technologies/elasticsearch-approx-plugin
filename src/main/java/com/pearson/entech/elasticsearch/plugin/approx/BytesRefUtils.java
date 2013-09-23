package com.pearson.entech.elasticsearch.plugin.approx;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.pearson.entech.elasticsearch.search.facet.approx.termlist.Constants;

/**
 * Utilities for handling BytesRef objects and data structures containing them.
 */
public class BytesRefUtils {

    /** The compact() method on BytesRefHash objects. */
    private static volatile Method __compact;

    /** An empty array of objects: the params required for reflective-invocation of __compact. */
    private static final Object[] __emptyParams = {};

    /**
     * A procedure for processing entries in a BytesRefHash.
     */
    public static interface Procedure {

        /**
         * Called once for each BytesRef.
         * 
         * @param ref the BytesRef
         * @throws Exception
         */
        void consume(BytesRef ref) throws Exception;

    }

    /**
     * Helper method to get the package-scoped compact() method of BytesRefHash.
     * 
     * @return the method reference
     */
    private static Method getCompactMethod() {
        if(__compact != null)
            return __compact;
        Method compact;
        try {
            compact = BytesRefHash.class.getDeclaredMethod("compact", new Class[0]);
        } catch(final SecurityException e) {
            throw new RuntimeException(e);
        } catch(final NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        compact.setAccessible(true);
        return __compact = compact;
    }

    /**
     * Destructively process the entries in a BytesRefHash, calling a procedure once for
     * each distinct entry's value. After processing, the hash will be emptied.
     * 
     * @param hash the hash to process
     * @param proc the procedure to call on each entry
     * @throws IllegalStateException if the procedure threw any exception
     */
    public static void process(final BytesRefHash hash, final Procedure proc) {
        int[] ids;
        try {
            ids = (int[]) getCompactMethod().invoke(hash, __emptyParams);
        } catch(final IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch(final InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        final BytesRef scratch = new BytesRef();
        for(int i = 0; i < ids.length; i++) {
            final int id = ids[i];
            if(id < 0)
                break;
            hash.get(id, scratch);
            try {
                proc.consume(scratch);
            } catch(final Exception e) {
                throw new IllegalStateException(e);
            }
        }
        hash.clear();
    }

    /**
     * Merge multiple BytesRefHash objects into the first one provided. All the others
     * will be emptied during this process.
     * 
     * @param hashes the hashes to merge
     */
    public static void merge(final BytesRefHash... hashes) {
        if(hashes.length < 1)
            throw new IllegalArgumentException("Cannot merge empty array of BytesRefHash objects");
        if(hashes.length == 1)
            return;
        final AddToHash proc = new AddToHash(hashes[0]);
        for(int i = 1; i < hashes.length; i++) {
            process(hashes[i], proc);
        }
    }

    /**
     * Serialize a hash to an ElasticSearch StreamOutput.
     * 
     * @param hash a BytesRefHash
     * @param out the StreamOutput
     * @throws IOException
     */
    public static void serialize(final BytesRefHash hash, final StreamOutput out) throws IOException {
        final ElasticSearchSerializer proc = new ElasticSearchSerializer(out, hash.size());
        try {
            process(hash, proc);
        } catch(final IllegalStateException e) {
            throw new IOException(e.getCause());
        }
    }

    /**
     * Deserialize a hash from an ElasticSearch StreamInput.
     * 
     * @param in the StreamInput 
     * @return a new BytesRefHash
     * @throws IOException 
     */
    public static BytesRefHash deserialize(final StreamInput in) throws IOException {
        final BytesRefHash output = new BytesRefHash();
        final int entries = in.readVInt();
        byte[] scratch = null;
        for(int i = 0; i < entries; i++) {
            final int length = in.readVInt();
            // Reuse previous byte array if long enough, otherwise create new one
            if(scratch == null || scratch.length < length) {
                scratch = new byte[length];
            }
            in.readBytes(scratch, 0, length);
            output.add(new BytesRef(scratch, 0, length));
        }
        return output;
    }

    /**
     * Procedure for adding BytesRefs to a BytesRefHash.
     */
    public static class AddToHash implements Procedure {

        private final BytesRefHash _target;

        /**
         * Create a new procedure.
         * 
         * @param target the BytesRefHash to merge into
         */
        public AddToHash(final BytesRefHash target) {
            _target = target;
        }

        @Override
        public void consume(final BytesRef ref) throws Exception {
            _target.add(ref);
        }

    }

    /**
     * Procedure for interpreting a BytesRefHash as a set of UTF8 strings.
     */
    public static class AsStrings implements Procedure {

        private final String[] _strings;

        private int _ptr;

        private final Constants.FIELD_DATA_TYPE _dataType;

        /**
         * Create a new procedure to extract strings into an array of the given size.
         * 
         * @param size the number of elements to accomodate
         */
        public AsStrings(final int size, final Constants.FIELD_DATA_TYPE dataType) {
            _strings = new String[size];
            _ptr = 0;
            _dataType = dataType;

        }

        @Override
        public void consume(final BytesRef ref) throws Exception {

            //check both ref length and data type before consuming the ref value
            if(ref.length == NumericUtils.BUF_SIZE_LONG && _dataType == Constants.FIELD_DATA_TYPE.LONG) {
                _strings[_ptr++] = Long.toString(NumericUtils.prefixCodedToLong(ref));
            }
            else if(ref.length == NumericUtils.BUF_SIZE_INT && _dataType == Constants.FIELD_DATA_TYPE.INT) {
                _strings[_ptr++] = Integer.toString(NumericUtils.prefixCodedToInt(ref));
            } else
                _strings[_ptr++] = ref.utf8ToString();

        }

        /**
         * Get an array containing all of the entries converted to strings.
         * 
         * @return the array
         */
        public String[] getArray() {
            return _strings;
        }

        /**
         * Get a list view of the array returned by getArray().
         * 
         * @return the list
         */
        public List<String> getList() {
            return Arrays.asList(_strings);
        }

    }

    /**
     * Procedure for serializing a BytesRefHash to an ElasticSearch StreamOutput.
     */
    public static class ElasticSearchSerializer implements Procedure {

        private final StreamOutput _out;

        /**
         * Create a new ElasticSearchSerializer.
         * 
         * @param out the StreamOutput to write to
         * @param entries the number of entries (BytesRef objects) to write
         * @throws IOException
         */
        public ElasticSearchSerializer(final StreamOutput out, final int entries) throws IOException {
            _out = out;
            _out.writeVInt(entries);
        }

        @Override
        public void consume(final BytesRef ref) throws Exception {
            _out.writeBytesRef(ref);
        }

    }

}
