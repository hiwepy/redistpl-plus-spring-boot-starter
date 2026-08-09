package org.springframework.data.redis.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Functions}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class FunctionsTest {

    @Test
    void toBooleanWithNull() {
        assertThat(Functions.TO_BOOLEAN.apply(null)).isNull();
    }

    @Test
    void toBooleanWithBoolean() {
        assertThat(Functions.TO_BOOLEAN.apply(true)).isTrue();
        assertThat(Functions.TO_BOOLEAN.apply(false)).isFalse();
    }

    @Test
    void toBooleanWithString() {
        assertThat(Functions.TO_BOOLEAN.apply("true")).isTrue();
        assertThat(Functions.TO_BOOLEAN.apply("false")).isFalse();
    }

    @Test
    void toBigDecimalWithNull() {
        assertThat(Functions.TO_BIGDECIMAL.apply(null)).isNull();
    }

    @Test
    void toBigDecimalWithBigDecimal() {
        BigDecimal bd = new BigDecimal("123.45");
        assertThat(Functions.TO_BIGDECIMAL.apply(bd)).isEqualTo(bd);
    }

    @Test
    void toBigDecimalWithString() {
        assertThat(Functions.TO_BIGDECIMAL.apply("123.45")).isEqualTo(new BigDecimal("123.45"));
    }

    @Test
    void toByteWithNull() {
        assertThat(Functions.TO_BYTE.apply(null)).isNull();
    }

    @Test
    void toByteWithByte() {
        assertThat(Functions.TO_BYTE.apply((byte) 42)).isEqualTo((byte) 42);
    }

    @Test
    void toByteWithString() {
        assertThat(Functions.TO_BYTE.apply("42")).isEqualTo((byte) 42);
    }

    @Test
    void toCharacterWithNull() {
        assertThat(Functions.TO_CHARACTER.apply(null)).isNull();
    }

    @Test
    void toCharacterWithCharacter() {
        assertThat(Functions.TO_CHARACTER.apply('A')).isEqualTo('A');
    }

    @Test
    void toCharacterWithString() {
        assertThat(Functions.TO_CHARACTER.apply("B")).isEqualTo('B');
    }

    @Test
    void toDoubleWithNull() {
        assertThat(Functions.TO_DOUBLE.apply(null)).isNull();
    }

    @Test
    void toDoubleWithDouble() {
        assertThat(Functions.TO_DOUBLE.apply(3.14)).isEqualTo(3.14);
    }

    @Test
    void toDoubleWithString() {
        assertThat(Functions.TO_DOUBLE.apply("3.14")).isEqualTo(3.14);
    }

    @Test
    void toFloatWithNull() {
        assertThat(Functions.TO_FLOAT.apply(null)).isNull();
    }

    @Test
    void toFloatWithFloat() {
        assertThat(Functions.TO_FLOAT.apply(2.5f)).isEqualTo(2.5f);
    }

    @Test
    void toFloatWithString() {
        assertThat(Functions.TO_FLOAT.apply("2.5")).isEqualTo(2.5f);
    }

    @Test
    void toIntegerWithNull() {
        assertThat(Functions.TO_INTEGER.apply(null)).isNull();
    }

    @Test
    void toIntegerWithInteger() {
        assertThat(Functions.TO_INTEGER.apply(42)).isEqualTo(42);
    }

    @Test
    void toIntegerWithString() {
        assertThat(Functions.TO_INTEGER.apply("42")).isEqualTo(42);
    }

    @Test
    void toLongWithNull() {
        assertThat(Functions.TO_LONG.apply(null)).isNull();
    }

    @Test
    void toLongWithLong() {
        assertThat(Functions.TO_LONG.apply(100L)).isEqualTo(100L);
    }

    @Test
    void toLongWithString() {
        assertThat(Functions.TO_LONG.apply("100")).isEqualTo(100L);
    }

    @Test
    void toShortWithNull() {
        assertThat(Functions.TO_SHORT.apply(null)).isNull();
    }

    @Test
    void toShortWithShort() {
        assertThat(Functions.TO_SHORT.apply((short) 10)).isEqualTo((short) 10);
    }

    @Test
    void toShortWithString() {
        assertThat(Functions.TO_SHORT.apply("10")).isEqualTo((short) 10);
    }

    @Test
    void toStringWithNull() {
        assertThat(Functions.TO_STRING.apply(null)).isNull();
    }

    @Test
    void toStringWithValue() {
        assertThat(Functions.TO_STRING.apply(42)).isEqualTo("42");
        assertThat(Functions.TO_STRING.apply("hello")).isEqualTo("hello");
    }
}
