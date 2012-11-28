package com.pearson.entech.elasticsearch.plugin.approx.termlist;

import static com.google.common.collect.Sets.newHashSet;

import java.util.Set;

public class StringTermListPayload implements TermListPayload<String> {

    private final Set<String> _terms = newHashSet();

    @Override
    public void add(final String term) {
        _terms.add(term);
    }

    @Override
    public void addBulk(final String term) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addAll(final TermListPayload<String>... payloads) {
        for(final TermListPayload<String> payload : payloads) {
            _terms.addAll(payload.terms());
        }
    }

    @Override
    public Set<String> terms() {
        return _terms;
    }

}
