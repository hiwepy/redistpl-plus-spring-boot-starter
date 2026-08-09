package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RedisKey}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class RedisKeyTest {

    @Test
    void redisPrefix() {
        assertThat(RedisKey.REDIS_PREFIX).isEqualTo("rds");
    }

    @Test
    void delimiter() {
        assertThat(RedisKey.DELIMITER).isEqualTo(":");
    }

    @Test
    void getKeyStrWithArgs() {
        String key = RedisKey.getKeyStr("arg1", "arg2");
        assertThat(key).isEqualTo("rds:arg1:arg2");
    }

    @Test
    void getKeyStrWithNullArgs() {
        String key = RedisKey.getKeyStr(null, "arg1", null, "arg2");
        assertThat(key).isEqualTo("rds:arg1:arg2");
    }

    @Test
    void getKeyStrWithEmptyArgs() {
        String key = RedisKey.getKeyStr("", "arg1", "  ", "arg2");
        assertThat(key).isEqualTo("rds:arg1:arg2");
    }

    @Test
    void getThreadKeyStr() {
        String key = RedisKey.getThreadKeyStr("prefix", "arg1", "arg2");
        assertThat(key).contains("prefix:");
        assertThat(key).contains("arg1");
        assertThat(key).contains("arg2");
    }

    @Test
    void getThreadKeyStrWithThreadId() {
        String key = RedisKey.getThreadKeyStr("prefix", "arg1");
        long threadId = Thread.currentThread().getId();
        assertThat(key).contains(String.valueOf(threadId));
    }

    @Test
    void geoLocationKey() {
        assertThat(RedisKey.GEO_LOCATION_KEY.getDesc()).isEqualTo("用户坐标");
        assertThat(RedisKey.GEO_LOCATION_KEY.getKey()).isEqualTo("rds:geo:location");
    }

    @Test
    void ipRegionInfo() {
        assertThat(RedisKey.IP_REGION_INFO.getDesc()).isEqualTo("用户坐标对应的地区编码缓存");
        assertThat(RedisKey.IP_REGION_INFO.getKey("192.168.1.1")).isEqualTo("rds:ip:region:192.168.1.1");
    }

    @Test
    void ipLocationInfo() {
        assertThat(RedisKey.IP_LOCATION_INFO.getDesc()).isEqualTo("用户坐标对应的地理位置缓存");
        assertThat(RedisKey.IP_LOCATION_INFO.getKey("192.168.1.1")).isEqualTo("rds:ip:location:192.168.1.1");
    }

    @Test
    void ipLocationBaiduInfo() {
        assertThat(RedisKey.IP_LOCATION_BAIDU_INFO.getKey("10.0.0.1")).isEqualTo("rds:baidu:ip:location:10.0.0.1");
    }

    @Test
    void ipLocationPconlineInfo() {
        assertThat(RedisKey.IP_LOCATION_PCONLINE_INFO.getKey("10.0.0.1")).isEqualTo("rds:pconline:ip:location:10.0.0.1");
    }

    @Test
    void enumValues() {
        assertThat(RedisKey.values()).hasSize(5);
        assertThat(RedisKey.valueOf("GEO_LOCATION_KEY")).isEqualTo(RedisKey.GEO_LOCATION_KEY);
    }
}
