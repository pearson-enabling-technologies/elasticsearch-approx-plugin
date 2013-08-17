package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class XContentEnabledList<E extends ToXContent>
        extends ArrayList<E> implements ToXContent {

    public XContentEnabledList() {
        super();
    }

    public XContentEnabledList(final Collection<? extends E> arg0) {
        super(arg0);
    }

    public XContentEnabledList(final int arg0) {
        super(arg0);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startArray();
        for(int i = 0; i < size(); i++)
            get(i).toXContent(builder, params);
        builder.endArray();
        return builder;
    }

}
