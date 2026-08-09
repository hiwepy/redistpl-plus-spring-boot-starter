package com.fasterxml.jackson.databind.json;

/**
 * JsonMapperBuilderCustomizer.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
@FunctionalInterface
public interface JsonMapperBuilderCustomizer {

    /**
     * Customize the JsonMapper.Builder.
     * @param jsonMapperBuilder the JsonMapper.Builder to customize
     */
    void customize(JsonMapper.Builder jsonMapperBuilder);

}
