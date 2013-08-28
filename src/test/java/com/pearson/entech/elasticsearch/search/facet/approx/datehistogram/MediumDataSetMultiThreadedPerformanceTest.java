package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MediumDataSetMultiThreadedPerformanceTest extends MediumDataSetPerformanceTest {

    private final ExecutorService _fiveThreads = Executors.newFixedThreadPool(5);

    @Override
    protected ExecutorService threadPool() {
        return _fiveThreads;
    }

}
