package org.springframework.data.redis.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Values}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class ValuesTest {

    @Test
    void nonNullWithNull() {
        assertThat(Values.nonNull(null)).isFalse();
    }

    @Test
    void nonNonNullWithValue() {
        assertThat(Values.nonNull("hello")).isTrue();
        assertThat(Values.nonNull(42)).isTrue();
        assertThat(Values.nonNull(true)).isTrue();
    }

    @Test
    void nonNullWithEmptyCollection() {
        assertThat(Values.nonNull(new ArrayList<>())).isFalse();
        assertThat(Values.nonNull(new HashSet<>())).isFalse();
    }

    @Test
    void nonNullWithNonEmptyCollection() {
        assertThat(Values.nonNull(Collections.singletonList("item"))).isTrue();
    }

    @Test
    void nonNullWithEmptyMap() {
        assertThat(Values.nonNull(new HashMap<>())).isFalse();
    }

    @Test
    void nonNullWithNonEmptyMap() {
        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        assertThat(Values.nonNull(map)).isTrue();
    }
}
