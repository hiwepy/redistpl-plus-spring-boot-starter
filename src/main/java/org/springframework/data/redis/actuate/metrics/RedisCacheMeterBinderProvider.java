package org.springframework.data.redis.actuate.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.springframework.boot.actuate.metrics.cache.CacheMeterBinderProvider;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.data.redis.cache.RedisCache;

public class RedisCacheMeterBinderProvider implements CacheMeterBinderProvider<RedisCache> {

    @Override
    public MeterBinder getMeterBinder(RedisCache cache, Iterable<Tag> tags) {
        return new RedisCacheMetrics(cache.getNativeCache(), cache.getName(), tags);
    }

}