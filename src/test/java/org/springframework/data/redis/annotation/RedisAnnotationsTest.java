package org.springframework.data.redis.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for Redis annotations.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 */
class RedisAnnotationsTest {

    @Test
    void redisChannelTopicAnnotation() throws Exception {
        @RedisChannelTopic("test-topic")
        class AnnotatedClass {}
        RedisChannelTopic annotation = AnnotatedClass.class.getAnnotation(RedisChannelTopic.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.value()).isEqualTo("test-topic");
    }

    @Test
    void redisPatternTopicAnnotation() throws Exception {
        @RedisPatternTopic("test-pattern.*")
        class AnnotatedClass {}
        RedisPatternTopic annotation = AnnotatedClass.class.getAnnotation(RedisPatternTopic.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.value()).isEqualTo("test-pattern.*");
    }

    @Test
    void redisStreamConsumerAnnotation() throws Exception {
        @RedisStreamConsumer(
            streamKey = "my-stream",
            groupName = "my-group",
            consumerName = "my-consumer",
            readOffset = "0",
            autoAck = false,
            value = "test"
        )
        class AnnotatedClass {}
        RedisStreamConsumer annotation = AnnotatedClass.class.getAnnotation(RedisStreamConsumer.class);
        assertThat(annotation).isNotNull();
        assertThat(annotation.streamKey()).isEqualTo("my-stream");
        assertThat(annotation.groupName()).isEqualTo("my-group");
        assertThat(annotation.consumerName()).isEqualTo("my-consumer");
        assertThat(annotation.readOffset()).isEqualTo("0");
        assertThat(annotation.autoAck()).isFalse();
        assertThat(annotation.value()).isEqualTo("test");
    }

    @Test
    void redisStreamConsumerDefaultAutoAck() throws Exception {
        @RedisStreamConsumer(
            streamKey = "stream",
            groupName = "group",
            consumerName = "consumer",
            readOffset = "0",
            value = "test"
        )
        class AnnotatedClass {}
        RedisStreamConsumer annotation = AnnotatedClass.class.getAnnotation(RedisStreamConsumer.class);
        assertThat(annotation.autoAck()).isTrue();
    }
}
