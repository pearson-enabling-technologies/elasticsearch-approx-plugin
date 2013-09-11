package com.pearson.entech.elasticsearch.search.facet.approx.date.external;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

/**
 * An ArrayList that implements ToXContent too. Has a name
 * attribute which is used as its fieldname in XContent output. Then
 * the list elements are rendered as an XContent list using their own
 * toXContent() methods.
 * 
 * @param <E> list element type; must in turn implement ToXContent
 */
public class XContentEnabledList<E extends ToXContent>
        extends ArrayList<E> implements ToXContent {

    private static final long serialVersionUID = 1L;

    private final String _name;

    private final XContentBuilderString _xName;

    /**
     * Create a list by copying in the values of the other collection.
     * 
     * @param data the collection to copy
     * @param name the name of the new list
     */
    public XContentEnabledList(final Collection<? extends E> data, final String name) {
        super(data);
        _name = name;
        _xName = null;
    }

    /**
     * Create a list with the initial capacity specified.
     * 
     * @param initialCapacity the starting capacity
     * @param name the name of the new list
     */
    public XContentEnabledList(final int initialCapacity, final String name) {
        super(initialCapacity);
        _name = name;
        _xName = null;
    }

    /**
     * Create an empty list.
     * 
     * @param name the name of the new list
     */
    public XContentEnabledList(final String name) {
        _name = name;
        _xName = null;
    }

    /**
     * Create a list with the initial capacity specified.
     * 
     * @param initialCapacity the starting capacity
     * @param name the name of the new list, as XContent
     */
    public XContentEnabledList(final int initialCapacity, final XContentBuilderString name) {
        super(initialCapacity);
        _name = null;
        _xName = name;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        if(_xName == null)
            builder.startArray(_name);
        else
            builder.startArray(_xName);
        for(int i = 0; i < size(); i++)
            get(i).toXContent(builder, params);
        builder.endArray();
        return builder;
    }

}
