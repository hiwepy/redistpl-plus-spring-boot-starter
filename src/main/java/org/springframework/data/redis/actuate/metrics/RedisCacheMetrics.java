package org.springframework.data.redis.actuate.metrics;

import com.google.common.cache.Cache;
import io.micrometer.common.lang.Nullable;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;
import org.springframework.cglib.core.internal.LoadingCache;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class RedisCacheMetrics<K, V, C extends Cache<K, V>> extends CacheMeterBinder<C> {

    /**
     * Creates a new {@link RedisCacheMetrics} instance.
     * @param cache The cache to be instrumented. You must call
     * {@link Caffeine#recordStats()} prior to building the cache for metrics to be
     * recorded.
     * @param cacheName Will be used to tag metrics with "cache".
     * @param tags tags to apply to all recorded metrics.
     */
    public RedisCacheMetrics(C cache, String cacheName, Iterable<Tag> tags) {
        super(cache, cacheName, tags);
    }

    /**
     * Record metrics on a Caffeine cache. You must call {@link Caffeine#recordStats()}
     * prior to building the cache for metrics to be recorded.
     * @param registry The registry to bind metrics to.
     * @param cache The cache to instrument.
     * @param cacheName Will be used to tag metrics with "cache".
     * @param tags Tags to apply to all recorded metrics. Must be an even number of
     * arguments representing key/value pairs of tags.
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @param <C> The cache type.
     * @return The instrumented cache, unchanged. The original cache is not wrapped or
     * proxied in any way.
     */
    public static <K, V, C extends Cache<K, V>> C monitor(MeterRegistry registry, C cache, String cacheName,
                                                          String... tags) {
        return monitor(registry, cache, cacheName, Tags.of(tags));
    }

    /**
     * Record metrics on a Caffeine cache. You must call {@link Caffeine#recordStats()}
     * prior to building the cache for metrics to be recorded.
     * @param registry The registry to bind metrics to.
     * @param cache The cache to instrument.
     * @param cacheName Will be used to tag metrics with "cache".
     * @param tags Tags to apply to all recorded metrics.
     * @param <K> Cache key type.
     * @param <V> Cache value type.
     * @param <C> The cache type.
     * @return The instrumented cache, unchanged. The original cache is not wrapped or
     * proxied in any way.
     * @see CacheStats
     */
    public static <K, V, C extends Cache<K, V>> C monitor(MeterRegistry registry, C cache, String cacheName,
                                                          Iterable<Tag> tags) {
        new RedisCacheMetrics<>(cache, cacheName, tags).bindTo(registry);
        return cache;
    }

    @Override
    protected Long size() {
        return getOrDefault(Cache::estimatedSize, null);
    }

    @Override
    protected long hitCount() {
        return getOrDefault(c -> c.stats().hitCount(), 0L);
    }

    @Override
    protected Long missCount() {
        return getOrDefault(c -> c.stats().missCount(), null);
    }

    @Override
    protected Long evictionCount() {
        return getOrDefault(c -> c.stats().evictionCount(), null);
    }

    @Override
    protected long putCount() {
        return getOrDefault(c -> c.stats().loadCount(), 0L);
    }

    @Override
    protected void bindImplementationSpecificMetrics(MeterRegistry registry) {
        C cache = getCache();
        FunctionCounter.builder("cache.eviction.weight", cache, c -> c.stats().evictionWeight())
                .tags(getTagsWithCacheName())
                .description("The sum of weights of evicted entries. This total does not include manual invalidations.")
                .register(registry);

        if (cache instanceof LoadingCache) {
            // dividing these gives you a measure of load latency
            TimeGauge.builder("cache.load.duration", cache, TimeUnit.NANOSECONDS, c -> c.stats().totalLoadTime())
                    .tags(getTagsWithCacheName()).description("The time the cache has spent loading new values")
                    .register(registry);

            FunctionCounter.builder("cache.load", cache, c -> c.stats().loadSuccessCount()).tags(getTagsWithCacheName())
                    .tags("result", "success").description(DESCRIPTION_CACHE_LOAD).register(registry);

            FunctionCounter.builder("cache.load", cache, c -> c.stats().loadFailureCount()).tags(getTagsWithCacheName())
                    .tags("result", "failure").description(DESCRIPTION_CACHE_LOAD).register(registry);
        }
    }

    @Nullable
    private Long getOrDefault(Function<C, Long> function, @Nullable Long defaultValue) {
        C cache = getCache();
        if (cache != null) {
            return function.apply(cache);
        }

        return defaultValue;
    }

    private long getOrDefault(ToLongFunction<C> function, long defaultValue) {
        C cache = getCache();
        if (cache != null) {
            return function.applyAsLong(cache);
        }

        return defaultValue;
    }

}
