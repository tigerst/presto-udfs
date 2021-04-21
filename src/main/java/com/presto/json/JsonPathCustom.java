package com.presto.json;

import io.airlift.slice.Slice;

public class JsonPathCustom {

    private final JsonExtract.JsonExtractor<Slice> scalarExtractor;
    private final JsonExtract.JsonExtractor<Slice> objectExtractor;
    private final JsonExtract.JsonExtractor<Long> sizeExtractor;

    public JsonPathCustom(String pattern) {
        scalarExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.ScalarValueJsonExtractor());
        objectExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.JsonValueJsonExtractor());
        sizeExtractor = JsonExtract.generateExtractor(pattern, new JsonExtract.JsonSizeExtractor());
    }

    public JsonExtract.JsonExtractor<Slice> getScalarExtractor() {
        return scalarExtractor;
    }

    public JsonExtract.JsonExtractor<Slice> getObjectExtractor() {
        return objectExtractor;
    }

    public JsonExtract.JsonExtractor<Long> getSizeExtractor() {
        return sizeExtractor;
    }
}
