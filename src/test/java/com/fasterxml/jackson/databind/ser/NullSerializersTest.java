package com.fasterxml.jackson.databind.ser;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for null JSON serializers.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class NullSerializersTest {

    @Test
    void nullStringSerializerInstance() {
        assertThat(NullStringJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullStringSerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullStringJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("");
    }

    @Test
    void nullNumberSerializerInstance() {
        assertThat(NullNumberJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullNumberSerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullNumberJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("0");
    }

    @Test
    void nullBooleanSerializerInstance() {
        assertThat(NullBooleanJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullBooleanSerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullBooleanJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("false");
    }

    @Test
    void nullArraySerializerInstance() {
        assertThat(NullArrayJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullArraySerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullArrayJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("[]");
    }

    @Test
    void nullObjectSerializerInstance() {
        assertThat(NullObjectJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullObjectSerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullObjectJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("{}");
    }

    @Test
    void nullDateSerializerInstance() {
        assertThat(NullDateJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullDateSerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullDateJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("");
    }

    @Test
    void nullJsonSerializerInstance() {
        assertThat(NullJsonSerializer.INSTANCE).isNotNull();
    }

    @Test
    void nullJsonSerializerSerialize() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SerializerProvider provider = mapper.getSerializerProvider();
        JsonFactory factory = new JsonFactory();
        java.io.StringWriter writer = new java.io.StringWriter();
        JsonGenerator gen = factory.createGenerator(writer);
        NullJsonSerializer.INSTANCE.serialize(null, gen, provider);
        gen.flush();
        assertThat(writer.toString()).contains("null");
    }
}
