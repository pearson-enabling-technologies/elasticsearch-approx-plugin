package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import java.util.Iterator;

import org.elasticsearch.search.facet.FacetExecutor.Collector;

/**
 * An extension to Collector that provides a standard Java Iterator/Iterable interface.
 * remove() is not supported.
 * 
 * @param <T> the data type of the field data
 */
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
