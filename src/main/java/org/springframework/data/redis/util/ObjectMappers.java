package org.springframework.data.redis.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.json.JsonMapperBuilderCustomizer;
import hitool.core.lang3.time.DateFormats;
import org.springframework.data.redis.core.RedisOperationException;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ObjectMappers {

    /**
     * 单独初始化ObjectMapper，不使用全局对象，保持缓存序列化的独立配置
     * @return ObjectMapper
     */
    public static ObjectMapper defaultObjectMapper(List<JsonMapperBuilderCustomizer> customizers) {

        JsonMapper.Builder builder = JsonMapper.builder()
                // 指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会跑出异常
                //.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL)
                .defaultDateFormat(new SimpleDateFormat(DateFormats.DATE_LONGFORMAT))
                // 指定要序列化的域，field,get和set,以及修饰符范围，ANY是都有包括private和public
                .visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                // 为objectMapper注册一个带有SerializerModifier的Factory
                //.serializerFactory(BeanSerializerFactory.instance.withSerializerModifier(new RedisBeanSerializerModifier()))
                ;

        for (JsonMapperBuilderCustomizer customizer : customizers) {
            customizer.customize(builder);
        }

        return builder.build();
    }

    public static final Function<Object, Boolean> TO_BOOLEAN = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Boolean ? (Boolean) value : Boolean.valueOf(value.toString());
    };

    public static final Function<Object, BigDecimal> TO_BIGDECIMAL = member -> {
        if(Objects.isNull(member)) {
            return null;
        }
        return member instanceof BigDecimal ? (BigDecimal) member : new BigDecimal(member.toString());
    };

    public static final Function<Object, Byte> TO_BYTE = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Byte ? (Byte) value : Byte.valueOf(value.toString());
    };

    public static final Function<Object, Character> TO_CHARACTER = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Character ? (Character) value : Character.valueOf(value.toString().charAt(0));
    };

    public static final Function<Object, Double> TO_DOUBLE = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Double ? (Double) value : Double.valueOf(value.toString());
    };

    public static final Function<Object, Float> TO_FLOAT = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Float ? (Float) value : Float.valueOf(value.toString());
    };

    public static final Function<Object, Integer> TO_INTEGER = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Integer ? (Integer) value : Integer.valueOf(value.toString());
    };

    public static final Function<Object, Long> TO_LONG = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Long ? (Long) value : Long.valueOf(value.toString());
    };

    public static final Function<Object, Short> TO_SHORT = value -> {
        if(Objects.isNull(value)) {
            return null;
        }
        return value instanceof Short ? (Short) value : Short.valueOf(value.toString());
    };

    public static final Function<Object, String> TO_STRING = value -> Objects.toString(value, null);


    private static final Map<Class<?>, Function<Object, ?>> classMapperFunctionCache = new ConcurrentHashMap<Class<?>, Function<Object, ?>>();

    private static final Map<Type, Function<Object, ?>> typeMapperFunctionCache = new ConcurrentHashMap<Type, Function<Object, ?>>();

    static {
        classMapperFunctionCache.put(Boolean.class, TO_BOOLEAN);
        classMapperFunctionCache.put(BigDecimal.class, TO_BIGDECIMAL);
        classMapperFunctionCache.put(Byte.class, TO_BYTE);
        classMapperFunctionCache.put(Character.class, TO_CHARACTER);
        classMapperFunctionCache.put(Double.class, TO_DOUBLE);
        classMapperFunctionCache.put(Float.class, TO_FLOAT);
        classMapperFunctionCache.put(Integer.class, TO_INTEGER);
        classMapperFunctionCache.put(Long.class, TO_LONG);
        classMapperFunctionCache.put(Short.class, TO_SHORT);
        classMapperFunctionCache.put(String.class, TO_STRING);
    }

    public static <T> Function<Object, T> getMapperFor(ObjectMapper objectMapper, Class<T> valueType) {
        Function<Object, ?> mapperFunc = classMapperFunctionCache.get(valueType);
        if(Objects.isNull(mapperFunc)){
            mapperFunc = value -> {
                // 1、如果value为空，直接返回null
                if(Objects.isNull(value)) {
                    return null;
                }
                // 2、如果value的类型和valueType一致，直接返回value
                if (value.getClass().isAssignableFrom(valueType)){
                    return valueType.cast(value);
                }
                try {
                    // 3、如果 value 是 String 类型，则使用 readValue 方法将字符串转换成 valueType 类型
                    if(value instanceof String){
                        return objectMapper.readValue(value.toString(), valueType);
                    }
                    // 4、如果 value 是 其他对象类型，则使用 convertValue 方法将对象转换成 valueType 类型
                    return objectMapper.convertValue(value, valueType);
                } catch (JsonProcessingException e) {
                    throw new RedisOperationException(e.getMessage());
                }
            };
            classMapperFunctionCache.put(valueType, mapperFunc);
        }
        return (Function<Object, T>) mapperFunc;
    }

    public static <T> Function<Object, T> getMapperFor(ObjectMapper objectMapper, JavaType valueType) {
        Function<Object, ?> mapperFunc = classMapperFunctionCache.get(valueType.getRawClass());
        if(Objects.isNull(mapperFunc)){
            mapperFunc = value -> {
                // 1、如果value为空，直接返回null
                if(Objects.isNull(value)) {
                    return null;
                }
                // 2、如果value的类型和valueType一致，直接返回value
                if (value.getClass().isAssignableFrom(valueType.getRawClass())){
                    return valueType.getRawClass().cast(value);
                }
                try {
                    // 3、如果 value 是 String 类型，则使用 readValue 方法将字符串转换成 valueType 类型
                    if(value instanceof String){
                        return objectMapper.readValue(value.toString(), valueType);
                    }
                    // 4、如果 value 是 其他对象类型，则使用 convertValue 方法将对象转换成 valueType 类型
                    return objectMapper.convertValue(value, valueType);
                } catch (JsonProcessingException e) {
                    throw new RedisOperationException(e.getMessage());
                }
            };
            classMapperFunctionCache.put(valueType.getRawClass(), mapperFunc);
        }
        return (Function<Object, T>) mapperFunc;
    }

    public static <T> Function<Object, T> getMapperFor(ObjectMapper objectMapper, TypeReference<T> valueTypeRef) {
        Function<Object, ?> mapperFunc = typeMapperFunctionCache.get(valueTypeRef.getType());
        if(Objects.isNull(mapperFunc)){
            mapperFunc = value -> {
                // 1、如果value为空，直接返回null
                if(Objects.isNull(value)) {
                    return null;
                }
                // 2、如果value的类型和valueType一致，直接返回value
                if (valueTypeRef.getType() instanceof Class<?> && value.getClass().isAssignableFrom((Class<T>) valueTypeRef.getType())){
                    return ((Class<T>) valueTypeRef.getType()).cast(value);
                }
                try {
                    // 3、如果 value 是 String 类型，则使用 readValue 方法将字符串转换成 valueType 类型
                    if(value instanceof String){
                        return objectMapper.readValue(value.toString(), valueTypeRef);
                    }
                    // 4、如果 value 是 其他对象类型，则使用 convertValue 方法将对象转换成 valueType 类型
                    return objectMapper.convertValue(value, valueTypeRef);
                } catch (JsonProcessingException e) {
                    throw new RedisOperationException(e.getMessage());
                }
            };
            typeMapperFunctionCache.put(valueTypeRef.getType(), mapperFunc);
        }
        return (Function<Object, T>) mapperFunc;
    }

    public static <T> Function<Object, T> getMapperFor(ObjectMapper objectMapper, Type valueType) {
        if (valueType instanceof Class<?>) {
            return getMapperFor(objectMapper, (Class<T>) valueType);
        }
        Function<Object, ?> mapperFunc = typeMapperFunctionCache.get(valueType);
        if(Objects.isNull(mapperFunc)){
            mapperFunc = value -> {
                // 1、如果value为空，直接返回null
                if(Objects.isNull(value)) {
                    return null;
                }
                // 2、如果value的类型和valueType一致，直接返回value
                if (valueType instanceof Class<?> && value.getClass().isAssignableFrom((Class<T>) valueType)){
                    return ((Class<T>) valueType).cast(value);
                }
                try {
                    // 3、如果 value 是 String 类型，则使用 readValue 方法将字符串转换成 valueType 类型
                    if(value instanceof String){
                        return objectMapper.readValue(value.toString(), TypeReferences.getType(valueType));
                    }
                    // 4、如果 value 是 其他对象类型，则使用 convertValue 方法将对象转换成 valueType 类型
                    return objectMapper.convertValue(value, TypeReferences.getType(valueType));
                } catch (JsonProcessingException e) {
                    throw new RedisOperationException(e.getMessage());
                }
            };
            typeMapperFunctionCache.put(valueType, mapperFunc);
        }
        return (Function<Object, T>) mapperFunc;
    }

}
