package com.pearson.entech.elasticsearch.search.facet.approx.datehistogram;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MediumDataSetTenThreadsPerformanceTest extends MediumDataSetPerformanceTest {

    private final ExecutorService _tenThreads = Executors.newFixedThreadPool(10);

    @Override
    protected ExecutorService threadPool() {
        return _tenThreads;
    }

}
