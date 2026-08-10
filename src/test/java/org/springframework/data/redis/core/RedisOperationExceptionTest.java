package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RedisOperationException}.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class RedisOperationExceptionTest {

    @Test
    void constructorWithMessage() {
        RedisOperationException ex = new RedisOperationException("test error");
        assertThat(ex.getMessage()).isEqualTo("test error");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void constructorWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("root cause");
        RedisOperationException ex = new RedisOperationException("test error", cause);
        assertThat(ex.getMessage()).isEqualTo("test error");
        assertThat(ex.getCause()).isEqualTo(cause);
    }
}
