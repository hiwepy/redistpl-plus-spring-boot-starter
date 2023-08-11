package com.fasterxml.jackson.databind.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

@FunctionalInterface
public interface JsonMapperBuilderCustomizer {

    /**
     * Customize the JsonMapper.Builder.
     * @param jsonMapperBuilder the JsonMapper.Builder to customize
     */
    void customize(JsonMapper.Builder jsonMapperBuilder);

}
