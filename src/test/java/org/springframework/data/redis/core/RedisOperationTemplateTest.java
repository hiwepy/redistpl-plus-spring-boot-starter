package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.DataType;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for {@link RedisOperationTemplate}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class RedisOperationTemplateTest {

    private RedisTemplate<String, Object> redisTemplate;
    private ObjectMapper objectMapper;
    private RedisOperationTemplate template;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        objectMapper = new ObjectMapper();
        template = new RedisOperationTemplate(redisTemplate, objectMapper);

        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        when(redisTemplate.expire(anyString(), anyLong(), any())).thenReturn(true);
        when(redisTemplate.expire(anyString(), any(Duration.class))).thenReturn(true);
        when(redisTemplate.getExpire(anyString(), any())).thenReturn(100L);
        when(redisTemplate.getExpire(anyString())).thenReturn(100L);
        when(redisTemplate.keys(anyString())).thenReturn(new HashSet<>(Arrays.asList("key1", "key2")));
        when(redisTemplate.delete(anyString())).thenReturn(true);
        when(redisTemplate.delete(anyCollection())).thenReturn(2L);
        when(redisTemplate.type(anyString())).thenReturn(DataType.STRING);
        when(redisTemplate.randomKey()).thenReturn("randomKey");
        when(redisTemplate.persist(anyString())).thenReturn(true);
    }

    @Test
    void constructor() {
        assertThat(template).isNotNull();
        assertThat(template.getRedisTemplate()).isEqualTo(redisTemplate);
        assertThat(template.getObjectMapper()).isEqualTo(objectMapper);
    }

    @Test
    void hasKey() {
        assertThat(template.hasKey("test")).isTrue();
        verify(redisTemplate).hasKey("test");
    }

    @Test
    void hasKeyException() {
        when(redisTemplate.hasKey(anyString())).thenThrow(new RuntimeException("error"));
        assertThatThrownBy(() -> template.hasKey("test"))
                .isInstanceOf(RedisOperationException.class);
    }

    @Test
    void expireWithSeconds() {
        assertThat(template.expire("test", 60)).isTrue();
    }

    @Test
    void expireWithDuration() {
        assertThat(template.expire("test", Duration.ofSeconds(60))).isTrue();
    }

    @Test
    void getExpire() {
        assertThat(template.getExpire("test")).isEqualTo(100L);
    }

    @Test
    void getExpireWithUnit() {
        assertThat(template.getExpire("test", TimeUnit.SECONDS)).isEqualTo(100L);
    }

    @Test
    void keysWithNullPattern() {
        assertThat(template.keys(null)).isNull();
    }

    @Test
    void type() {
        assertThat(template.type("test")).isEqualTo(DataType.STRING);
    }

    @Test
    void randomKey() {
        assertThat(template.randomKey()).isEqualTo("randomKey");
    }

    @Test
    void persist() {
        assertThat(template.persist("test")).isTrue();
    }

    @Test
    void rename() {
        doNothing().when(redisTemplate).rename(anyString(), anyString());
        template.rename("old", "new");
        verify(redisTemplate).rename("old", "new");
    }

    @Test
    void renameIfAbsent() {
        when(redisTemplate.renameIfAbsent(anyString(), anyString())).thenReturn(true);
        assertThat(template.renameIfAbsent("old", "new")).isTrue();
    }
}
