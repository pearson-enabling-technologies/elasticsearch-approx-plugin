package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.concurrent.Callable;

import org.elasticsearch.action.search.SearchResponse;

public interface RandomQuery<C extends CountingQueryResultChecker> extends Callable<SearchResponse> {

    C buildChecker();

    C getChecker();

    @Override
    SearchResponse call() throws Exception;

    void checkResults(SearchResponse myResponse);

}