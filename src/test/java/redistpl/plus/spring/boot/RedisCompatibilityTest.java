package redistpl.plus.spring.boot;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.GeoTemplate;
import org.springframework.data.redis.core.MapUtils;
import org.springframework.data.redis.core.RedisKey;
import org.springframework.data.redis.core.RedisOperationTemplate;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RedisCompatibilityTest {
    @Test void shouldConstructBackportedRedisOperationTemplate() {
        assertNotNull(new RedisOperationTemplate(new RedisTemplate<String, Object>(), new ObjectMapper()));
    }
    @Test void shouldBuildStableRedisKeys() {
        assertEquals("rds:region:127.0.0.1", RedisKey.getKeyStr("region", "127.0.0.1"));
        assertEquals("rds:region", RedisKey.getKeyStr(null, "", "region"));
        assertNotNull(RedisKey.LOCK_KEY.getKey("lock"));
        assertNotNull(RedisKey.BLOCKING_LOCK_KEY.getKey("lock"));
        assertNotNull(RedisKey.GEO_LOCATION_KEY.getKey());
        assertEquals("rds:ip:region:127.0.0.1", RedisKey.IP_REGION_INFO.getKey("127.0.0.1"));
        assertNotNull(RedisKey.IP_LOCATION_INFO.getDesc());
        assertNotNull(RedisKey.IP_LOCATION_INFO.getKey("127.0.0.1"));
        assertNotNull(RedisKey.IP_LOCATION_BAIDU_INFO.getKey("127.0.0.1"));
        assertNotNull(RedisKey.IP_LOCATION_PCONLINE_INFO.getKey("127.0.0.1"));
        assertNotNull(RedisKey.getThreadKeyStr("thread", null, "", "value"));
        RedisKey.main(new String[0]);
    }
    @Test void shouldReadMapValueAsString() {
        assertNotNull(new MapUtils());
        assertEquals("42", MapUtils.getString(Collections.singletonMap("answer", 42), "answer"));
        assertEquals(null, MapUtils.getString(null, "answer"));
    }
    @Test void shouldCalculateZeroDistanceForSamePoint() {
        assertEquals(0.0D, new GeoTemplate().getDistance(39.9D, 116.4D, 39.9D, 116.4D), 0.001D);
    }
}
