package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MapUtils}.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class MapUtilsTest {

    @Test
    void getStringFromNullMap() {
        assertThat(MapUtils.getString(null, "key")).isNull();
    }

    @Test
    void getStringWithMissingKey() {
        Map<String, String> map = new HashMap<>();
        assertThat(MapUtils.getString(map, "missing")).isNull();
    }

    @Test
    void getStringWithExistingKey() {
        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        assertThat(MapUtils.getString(map, "key")).isEqualTo("value");
    }

    @Test
    void getStringWithNonStringValue() {
        Map<String, Integer> map = new HashMap<>();
        map.put("key", 42);
        assertThat(MapUtils.getString(map, "key")).isEqualTo("42");
    }
}
