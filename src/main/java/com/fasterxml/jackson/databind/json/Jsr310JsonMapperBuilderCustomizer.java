package com.fasterxml.jackson.databind.json;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import hitool.core.lang3.time.DateFormats;
import org.springframework.core.Ordered;
import redistpl.plus.spring.boot.RedisJacksonProperties;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class Jsr310JsonMapperBuilderCustomizer implements JsonMapperBuilderCustomizer, Ordered {

    private final Map<Class<?>, JsonSerializer<?>> serializers = new LinkedHashMap<>();

    private final Map<Class<?>, JsonDeserializer<?>> deserializers = new LinkedHashMap<>();
    private final RedisJacksonProperties jacksonProperties;

    public Jsr310JsonMapperBuilderCustomizer(RedisJacksonProperties jacksonProperties) {
        this.jacksonProperties = jacksonProperties;
    }

    @Override
    public void customize(JsonMapper.Builder builder) {

        JavaTimeModule module = new JavaTimeModule();
        module.addSerializer(LocalDateTime.class, localDateTimeSerializer());
        module.addDeserializer(LocalDateTime.class, localDateTimeDeserializer());
        addSerializers(module);
        addDeserializers(module);
        builder.addModule(module);

    }

    public LocalDateTimeSerializer localDateTimeSerializer() {
        String pattern = Optional.ofNullable(this.jacksonProperties.getDateFormat()).orElse(DateFormats.DATE_LONGFORMAT);
        return new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(pattern));
    }

    public LocalDateTimeDeserializer localDateTimeDeserializer() {
        String pattern = Optional.ofNullable(this.jacksonProperties.getDateFormat()).orElse(DateFormats.DATE_LONGFORMAT);
        return new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(pattern));
    }

    /**
     * Configure custom serializers. Each serializer is registered for the type
     * returned by {@link JsonSerializer#handledType()}, which must not be {@code null}.
     * @see #serializersByType(Map)
     */
    public Jsr310JsonMapperBuilderCustomizer serializers(JsonSerializer<?>... serializers) {
        for (JsonSerializer<?> serializer : serializers) {
            Class<?> handledType = serializer.handledType();
            if (handledType == null || handledType == Object.class) {
                throw new IllegalArgumentException("Unknown handled type in " + serializer.getClass().getName());
            }
            this.serializers.put(serializer.handledType(), serializer);
        }
        return this;
    }

    /**
     * Configure a custom serializer for the given type.
     * @since 4.1.2
     * @see #serializers(JsonSerializer...)
     */
    public Jsr310JsonMapperBuilderCustomizer serializerByType(Class<?> type, JsonSerializer<?> serializer) {
        this.serializers.put(type, serializer);
        return this;
    }

    /**
     * Configure custom serializers for the given types.
     * @see #serializers(JsonSerializer...)
     */
    public Jsr310JsonMapperBuilderCustomizer serializersByType(Map<Class<?>, JsonSerializer<?>> serializers) {
        this.serializers.putAll(serializers);
        return this;
    }

    /**
     * Configure custom deserializers. Each deserializer is registered for the type
     * returned by {@link JsonDeserializer#handledType()}, which must not be {@code null}.
     * @since 4.3
     * @see #deserializersByType(Map)
     */
    public Jsr310JsonMapperBuilderCustomizer deserializers(JsonDeserializer<?>... deserializers) {
        for (JsonDeserializer<?> deserializer : deserializers) {
            Class<?> handledType = deserializer.handledType();
            if (handledType == null || handledType == Object.class) {
                throw new IllegalArgumentException("Unknown handled type in " + deserializer.getClass().getName());
            }
            this.deserializers.put(deserializer.handledType(), deserializer);
        }
        return this;
    }

    /**
     * Configure a custom deserializer for the given type.
     * @since 4.1.2
     */
    public Jsr310JsonMapperBuilderCustomizer deserializerByType(Class<?> type, JsonDeserializer<?> deserializer) {
        this.deserializers.put(type, deserializer);
        return this;
    }

    /**
     * Configure custom deserializers for the given types.
     */
    public Jsr310JsonMapperBuilderCustomizer deserializersByType(Map<Class<?>, JsonDeserializer<?>> deserializers) {
        this.deserializers.putAll(deserializers);
        return this;
    }

    private <T> void addSerializers(SimpleModule module) {
        serializers.forEach((type, serializer) ->
                module.addSerializer((Class<? extends T>) type, (JsonSerializer<T>) serializer));
    }

    @SuppressWarnings("unchecked")
    private <T> void addDeserializers(SimpleModule module) {
        this.deserializers.forEach((type, deserializer) ->
                module.addDeserializer((Class<T>) type, (JsonDeserializer<? extends T>) deserializer));
    }

    @Override
    public int getOrder() {
        return 1;
    }

}
