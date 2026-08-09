package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RedisKeyConstant}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class RedisKeyConstantTest {

    @Test
    void geoLocationKey() {
        assertThat(RedisKeyConstant.GEO_LOCATION_KEY).isEqualTo("geo:location");
    }

    @Test
    void ipRegionKey() {
        assertThat(RedisKeyConstant.IP_REGION_KEY).isEqualTo("ip:region");
    }

    @Test
    void ipLocationKey() {
        assertThat(RedisKeyConstant.IP_LOCATION_KEY).isEqualTo("ip:location");
    }

    @Test
    void ipBaiduLocationKey() {
        assertThat(RedisKeyConstant.IP_BAIDU_LOCATION_KEY).isEqualTo("baidu:ip:location");
    }

    @Test
    void ipPconlineLocationKey() {
        assertThat(RedisKeyConstant.IP_PCONLINE_LOCATION_KEY).isEqualTo("pconline:ip:location");
    }
}
