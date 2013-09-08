package com.pearson.entech.elasticsearch.search.facet.approx.date;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.pearson.entech.elasticsearch.search.facet.approx.date.internal.MediumDataSetPerformanceTest;

public class MediumDataSetSingleThreadedPerformanceTest extends MediumDataSetPerformanceTest {

    private final ExecutorService _singleThread = Executors.newSingleThreadExecutor();

    @Override
    protected ExecutorService threadPool() {
        return _singleThread;
    }

}
