package com.pearson.entech.elasticsearch.search.facet.approx.date.collectors;

import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;

/**
 * Placeholder for classes which have a type parameter for the value_field data,
 * for cases where that option isn't in use.
 */
public interface NullFieldData extends AtomicFieldData<ScriptDocValues.Empty> {

}
