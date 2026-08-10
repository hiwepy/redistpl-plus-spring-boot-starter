package com.fasterxml.jackson.databind.ser;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Tests for Null*JsonSerializer classes.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class NullJsonSerializersTest {

    @Test
    void nullJsonSerializerInstance() {
        assertThat(NullJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullStringJsonSerializerInstance() {
        assertThat(NullStringJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullNumberJsonSerializerInstance() {
        assertThat(NullNumberJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullBooleanJsonSerializerInstance() {
        assertThat(NullBooleanJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullArrayJsonSerializerInstance() {
        assertThat(NullArrayJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullObjectJsonSerializerInstance() {
        assertThat(NullObjectJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullDateJsonSerializerInstance() {
        assertThat(NullDateJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullStringSerializerReturnsEmptyString() throws IOException {
        com.fasterxml.jackson.databind.json.JsonMapper mapper = new com.fasterxml.jackson.databind.json.JsonMapper();
        String result = mapper.writeValueAsString(null);
        assertThat(result).isEqualTo("null");
    }
}
