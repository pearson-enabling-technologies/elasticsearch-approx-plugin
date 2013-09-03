package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;

public interface NullFieldData extends AtomicFieldData<ScriptDocValues.Empty> {

}
