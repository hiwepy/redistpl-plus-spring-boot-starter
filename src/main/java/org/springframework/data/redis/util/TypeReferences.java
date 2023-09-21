package org.springframework.data.redis.util;

import com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class TypeReferences {

    public static final MapTypeReference MAP_TYPE = new MapTypeReference();

    public static final ListTypeReference LIST_TYPE = new ListTypeReference();

    public static final StringTypeReference STRING_TYPE = new StringTypeReference();

    public static final IntegerTypeReference INTEGER_TYPE = new IntegerTypeReference();

    public static final LongTypeReference LONG_TYPE = new LongTypeReference();

    public static final DoubleTypeReference DOUBLE_TYPE = new DoubleTypeReference();

    public static class MapTypeReference extends TypeReference<Map<String, Object>> {

    }

    public static class ListTypeReference extends TypeReference<List<Object>> {

    }

    public static class StringTypeReference extends TypeReference<String> {

    }

    public static class IntegerTypeReference extends TypeReference<Integer> {

    }

    public static class LongTypeReference extends TypeReference<Long> {

    }

    public static class DoubleTypeReference extends TypeReference<Double> {

    }


    public static class JavaTypeReference extends TypeReference<Type> {

        protected Type _type;
        public JavaTypeReference(Type type) {
            _type = type;
        }

        @Override
        public Type getType() {
            return _type;
        }
    }

    private static final Map<String, TypeReference<?>> typeReferenceCache = new ConcurrentHashMap<String, TypeReference<?>>();
    private static final Map<String, TypeReference<?>> listTypeReferenceCache = new ConcurrentHashMap<String, TypeReference<?>>();
    private static final Map<Type, TypeReference<?>> javaTypeReferenceCache = new ConcurrentHashMap<Type, TypeReference<?>>();

    public static <T> TypeReference<T> getType(Class<T> clazz){
        TypeReference<?> typeReference = typeReferenceCache.get(clazz.getName());
        if(Objects.isNull(typeReference)){
            typeReference = new TypeReference<T>() {};
            typeReferenceCache.put(clazz.getName(), typeReference);
        }
        return (TypeReference<T>) typeReference;
    }

    public static <T> TypeReference<T> getType(Type valueType){
        if (valueType instanceof Class<?>) {
            return getType((Class<T>) valueType);
        }
        TypeReference<?> typeReference = javaTypeReferenceCache.get(valueType);
        if(Objects.isNull(typeReference)){
            typeReference = new JavaTypeReference(valueType);
            javaTypeReferenceCache.put(valueType, typeReference);
        }
        return (TypeReference<T>) typeReference;
    }

    public static <T> TypeReference<List<T>> getListType(Class<T> clazz){
        TypeReference<?> typeReference = listTypeReferenceCache.get(clazz.getName());
        if(Objects.isNull(typeReference)){
            typeReference = new TypeReference<List<T>>() {};
            listTypeReferenceCache.put(clazz.getName(), typeReference);
        }
        return (TypeReference<List<T>>) typeReference;
    }

}
