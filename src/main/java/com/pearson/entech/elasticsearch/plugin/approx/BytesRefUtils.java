package com.pearson.entech.elasticsearch.plugin.approx;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * Utilities for handling BytesRef objects and data structures containing them.
 */
public class BytesRefUtils {

    /** The compact() method on BytesRefHash objects. */
    private static Method __compact;

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
        try {
            __compact = BytesRefHash.class.getDeclaredMethod("compact", new Class[0]);
        } catch(final SecurityException e) {
            throw new RuntimeException(e);
        } catch(final NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        __compact.setAccessible(true);
        return __compact;
    }

    /**
     * Destructively process the entries in a BytesRefHash, calling a procedure once for
     * each distinct entry's value.
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

}
