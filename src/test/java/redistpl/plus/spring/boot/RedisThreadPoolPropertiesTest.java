package redistpl.plus.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RedisThreadPoolProperties}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class RedisThreadPoolPropertiesTest {

    @Test
    void defaultValues() {
        RedisThreadPoolProperties props = new RedisThreadPoolProperties();
        assertThat(props.getListener()).isNotNull();
        assertThat(props.getSubscription()).isNotNull();
        assertThat(props.getStream()).isNotNull();
    }

    @Test
    void setAndGetValues() {
        RedisThreadPoolProperties props = new RedisThreadPoolProperties();

        RedisThreadPoolProperties.Pool listener = new RedisThreadPoolProperties.Pool();
        props.setListener(listener);
        assertThat(props.getListener()).isEqualTo(listener);

        RedisThreadPoolProperties.Pool subscription = new RedisThreadPoolProperties.Pool();
        props.setSubscription(subscription);
        assertThat(props.getSubscription()).isEqualTo(subscription);

        RedisThreadPoolProperties.StreamPool stream = new RedisThreadPoolProperties.StreamPool();
        props.setStream(stream);
        assertThat(props.getStream()).isEqualTo(stream);
    }

    @Test
    void poolDefaultValues() {
        RedisThreadPoolProperties.Pool pool = new RedisThreadPoolProperties.Pool();
        assertThat(pool.getCoreSize()).isEqualTo(1);
        assertThat(pool.getMaxSize()).isGreaterThan(0);
        assertThat(pool.getQueueCapacity()).isEqualTo(Integer.MAX_VALUE);
        assertThat(pool.getKeepAlive()).isEqualTo(Duration.ofSeconds(60));
        assertThat(pool.isAllowCoreThreadTimeOut()).isFalse();
        assertThat(pool.getThreadNamePrefix()).isEqualTo("RedisAsyncTaskExecutor-");
        assertThat(pool.isDaemon()).isFalse();
        assertThat(pool.getRejectedPolicy()).isEqualTo(RedisThreadPoolProperties.RejectedPolicy.AbortPolicy);
    }

    @Test
    void poolSetAndGetValues() {
        RedisThreadPoolProperties.Pool pool = new RedisThreadPoolProperties.Pool();

        pool.setCoreSize(4);
        assertThat(pool.getCoreSize()).isEqualTo(4);

        pool.setMaxSize(8);
        assertThat(pool.getMaxSize()).isEqualTo(8);

        pool.setQueueCapacity(100);
        assertThat(pool.getQueueCapacity()).isEqualTo(100);

        pool.setKeepAlive(Duration.ofSeconds(120));
        assertThat(pool.getKeepAlive()).isEqualTo(Duration.ofSeconds(120));

        pool.setAllowCoreThreadTimeOut(true);
        assertThat(pool.isAllowCoreThreadTimeOut()).isTrue();

        pool.setThreadNamePrefix("my-pool-");
        assertThat(pool.getThreadNamePrefix()).isEqualTo("my-pool-");

        pool.setDaemon(true);
        assertThat(pool.isDaemon()).isTrue();

        pool.setRejectedPolicy(RedisThreadPoolProperties.RejectedPolicy.CallerRunsPolicy);
        assertThat(pool.getRejectedPolicy()).isEqualTo(RedisThreadPoolProperties.RejectedPolicy.CallerRunsPolicy);
    }

    @Test
    void streamPoolDefaultValues() {
        RedisThreadPoolProperties.StreamPool streamPool = new RedisThreadPoolProperties.StreamPool();
        assertThat(streamPool.getBatchSize()).isEqualTo(1);
        assertThat(streamPool.getPollTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(streamPool.getReadOffset()).isEqualTo(RedisThreadPoolProperties.ReadOffsetPolicy.lastConsumed);
    }

    @Test
    void streamPoolSetAndGetValues() {
        RedisThreadPoolProperties.StreamPool streamPool = new RedisThreadPoolProperties.StreamPool();

        streamPool.setBatchSize(10);
        assertThat(streamPool.getBatchSize()).isEqualTo(10);

        streamPool.setPollTimeout(Duration.ofSeconds(5));
        assertThat(streamPool.getPollTimeout()).isEqualTo(Duration.ofSeconds(5));

        streamPool.setReadOffset(RedisThreadPoolProperties.ReadOffsetPolicy.latest);
        assertThat(streamPool.getReadOffset()).isEqualTo(RedisThreadPoolProperties.ReadOffsetPolicy.latest);
    }

    @Test
    void readOffsetPolicyValues() {
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.values()).hasSize(3);
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.from).isNotNull();
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.latest).isNotNull();
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.lastConsumed).isNotNull();
    }

    @Test
    void readOffsetPolicyGetReadOffset() {
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.from.getReadOffset()).isNotNull();
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.latest.getReadOffset()).isNotNull();
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.lastConsumed.getReadOffset()).isNotNull();
        assertThat(RedisThreadPoolProperties.ReadOffsetPolicy.from.getReadOffset("0")).isNotNull();
    }

    @Test
    void rejectedPolicyValues() {
        assertThat(RedisThreadPoolProperties.RejectedPolicy.values()).hasSize(4);
        assertThat(RedisThreadPoolProperties.RejectedPolicy.AbortPolicy).isNotNull();
        assertThat(RedisThreadPoolProperties.RejectedPolicy.CallerRunsPolicy).isNotNull();
        assertThat(RedisThreadPoolProperties.RejectedPolicy.DiscardPolicy).isNotNull();
        assertThat(RedisThreadPoolProperties.RejectedPolicy.DiscardOldestPolicy).isNotNull();
    }

    @Test
    void rejectedPolicyHandlers() {
        assertThat(RedisThreadPoolProperties.RejectedPolicy.AbortPolicy.getRejectedExecutionHandler())
                .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
        assertThat(RedisThreadPoolProperties.RejectedPolicy.CallerRunsPolicy.getRejectedExecutionHandler())
                .isInstanceOf(ThreadPoolExecutor.CallerRunsPolicy.class);
        assertThat(RedisThreadPoolProperties.RejectedPolicy.DiscardPolicy.getRejectedExecutionHandler())
                .isInstanceOf(ThreadPoolExecutor.DiscardPolicy.class);
        assertThat(RedisThreadPoolProperties.RejectedPolicy.DiscardOldestPolicy.getRejectedExecutionHandler())
                .isInstanceOf(ThreadPoolExecutor.DiscardOldestPolicy.class);
    }
}
