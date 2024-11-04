package com.fasterxml.jackson.databind.json;

@FunctionalInterface
public interface JsonMapperBuilderCustomizer {

    /**
     * Customize the JsonMapper.Builder.
     * @param jsonMapperBuilder the JsonMapper.Builder to customize
     */
    void customize(JsonMapper.Builder jsonMapperBuilder);

}
