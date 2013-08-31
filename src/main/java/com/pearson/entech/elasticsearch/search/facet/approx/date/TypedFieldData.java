package com.pearson.entech.elasticsearch.search.facet.approx.date;

import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;

public class TypedFieldData {

    public final IndexFieldData<?> data;
    public final FieldDataType type;

    public TypedFieldData(final IndexFieldData<?> fieldData, final FieldDataType fieldDataType) {
        data = fieldData;
        type = fieldDataType;
    }

}
