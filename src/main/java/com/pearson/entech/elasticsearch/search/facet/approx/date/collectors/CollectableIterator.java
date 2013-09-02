package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.util.Iterator;

import org.elasticsearch.search.facet.FacetExecutor.Collector;

public abstract class CollectableIterator<T> extends Collector implements Iterator<T>, Iterable<T> {

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Can't remove from CollectableIterator");
    }

}
