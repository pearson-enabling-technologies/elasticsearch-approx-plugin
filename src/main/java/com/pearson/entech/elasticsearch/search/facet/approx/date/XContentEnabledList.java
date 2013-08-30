package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

public class XContentEnabledList<E extends ToXContent>
        extends ArrayList<E> implements ToXContent {

    private final String _name;

    private final XContentBuilderString _xName;

    public XContentEnabledList(final Collection<? extends E> data, final String name) {
        super(data);
        _name = name;
        _xName = null;
    }

    public XContentEnabledList(final int initialCapacity, final String name) {
        super(initialCapacity);
        _name = name;
        _xName = null;
    }

    public XContentEnabledList(final String name) {
        _name = name;
        _xName = null;
    }

    public XContentEnabledList(final int size, final XContentBuilderString name) {
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
