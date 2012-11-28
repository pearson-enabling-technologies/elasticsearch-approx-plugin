package com.pearson.entech.elasticsearch.plugin.approx.termlist;

import java.util.Set;

public interface TermListPayload<T> {

    void add(T term);

    void addBulk(T term);

    void addAll(TermListPayload<T>... payloads);

    Set<T> terms();

}
