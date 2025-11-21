package org.springframework.data.redis.util;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.function.Function;

public class Functions {

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

}
