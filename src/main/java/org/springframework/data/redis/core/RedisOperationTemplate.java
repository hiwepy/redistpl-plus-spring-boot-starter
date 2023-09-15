package org.springframework.data.redis.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisZSetCommands.*;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 1、基于RedisTemplate操作的二次封装
 * 2、参考：
 * https://blog.csdn.net/qq_24598601/article/details/105876432
 */
@SuppressWarnings({"unchecked","rawtypes"})
@Slf4j
public class RedisOperationTemplate extends AbstractOperations<String, Object> {

	private static final Long LOCK_SUCCESS = 1L;
    private static final Long LOCK_EXPIRED = -1L;


    private static final RedisScript<Long> LOCK_LUA_SCRIPT = RedisScript.of(RedisLua.LOCK_LUA_SCRIPT, Long.class );

    private static final RedisScript<Long> UNLOCK_LUA_SCRIPT = RedisScript.of(RedisLua.UNLOCK_LUA_SCRIPT, Long.class );

    public static final RedisScript<Long> INCR_SCRIPT = RedisScript.of(RedisLua.INCR_SCRIPT, Long.class);
    public static final RedisScript<Long> DECR_SCRIPT = RedisScript.of(RedisLua.DECR_SCRIPT, Long.class);
	public static final RedisScript<Long> DIV_SCRIPT = RedisScript.of(RedisLua.DIV_SCRIPT, Long.class);

    public static final RedisScript<Double> INCR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.INCR_BYFLOAT_SCRIPT, Double.class);
    public static final RedisScript<Double> DECR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.DECR_BYFLOAT_SCRIPT, Double.class);

    public static final RedisScript<Long> HINCR_SCRIPT = RedisScript.of(RedisLua.HINCR_SCRIPT, Long.class);
    public static final RedisScript<Long> HDECR_SCRIPT = RedisScript.of(RedisLua.HDECR_SCRIPT, Long.class);
	public static final RedisScript<Long> HDIV_SCRIPT = RedisScript.of(RedisLua.HDIV_SCRIPT, Long.class);

    public static final RedisScript<Double> HINCR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.HINCR_BYFLOAT_SCRIPT, Double.class);
    public static final RedisScript<Double> HDECR_BYFLOAT_SCRIPT = RedisScript.of(RedisLua.HDECR_BYFLOAT_SCRIPT, Double.class);

    public static final Function<Object, String> TO_STRING = member -> Objects.toString(member, null);


	protected <T> Function<Object, T> toObject(Class<T> clazz) {
		return value -> {
			if(Objects.isNull(value)) {
				return null;
			}
			if (value.getClass().isAssignableFrom(clazz)){
				return clazz.cast(value);
			}
			try {
				// hash 转 string
				String valueStr = value instanceof String ? value.toString() : getObjectMapper().writeValueAsString(value);
				// string 转 对象
				return getObjectMapper().readValue(valueStr, clazz);
			} catch (JsonProcessingException e) {
				throw new RedisOperationException(e.getMessage());
			}
		};
	}

	protected <T> Function<Object, T> toObject(TypeReference<T> typeRef) {
		return value -> {
			if(Objects.isNull(value)) {
				return null;
			}
			try {
				// hash 转 string
				String valueStr = value instanceof String ? value.toString() : getObjectMapper().writeValueAsString(value);
				// string 转 对象
				return getObjectMapper().readValue(valueStr, typeRef);
			} catch (JsonProcessingException e) {
				throw new RedisOperationException(e.getMessage());
			}
		};
	}

    public static final Function<Object, Double> TO_DOUBLE = member -> {
		if(Objects.isNull(member)) {
			return null;
		}
		return member instanceof Double ? (Double) member : new BigDecimal(member.toString()).doubleValue();
	};

	public static final Function<Object, Long> TO_LONG = member -> {
		if(Objects.isNull(member)) {
			return null;
		}
		return member instanceof Long ? (Long) member : new BigDecimal(member.toString()).longValue();
	};

	public static final Function<Object, Integer> TO_INTEGER = member -> {
		if(Objects.isNull(member)) {
			return null;
		}
		return member instanceof Integer ? (Integer) member : new BigDecimal(member.toString()).intValue();
	};

	private final RedisTemplate<String, Object> redisTemplate;
	private final ObjectMapper objectMapper;

	public RedisOperationTemplate(RedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper) {
		super(redisTemplate);
		this.redisTemplate = redisTemplate;
		this.objectMapper = objectMapper;
	}

	public RedisTemplate<String, Object> getRedisTemplate() {
		return redisTemplate;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	// =============================Serializer============================

	public byte[] getRawKey(Object key) {
		return rawKey(key);
	}

	public byte[] getRawString(String key) {
		return rawString(key);
	}

	public byte[] getRawValue(Object value) {
		return rawValue(value);
	}

	public <V> byte[][] getRawValues(Collection<V> values) {
		return rawValues(values);
	}

	public <HK> byte[] getRawHashKey(HK hashKey) {
		return rawHashKey(hashKey);
	}

	public <HK> byte[][] getRawHashKeys(HK... hashKeys) {
		return rawHashKeys(hashKeys);
	}

	public <HV> byte[] getRawHashValue(HV value) {
		return rawHashValue(value);
	}

	public byte[][] getRawKeys(String key, String otherKey) {
		return rawKeys(key, otherKey);
	}

	public byte[][] getRawKeys(Collection<String> keys) {
		return rawKeys(keys);
	}

	public byte[][] getRawKeys(String key, Collection<String> keys) {
		return rawKeys(key, keys);
	}

	// =============================Deserialize============================

	public Set<Object> getDeserializeValues(Set<byte[]> rawValues) {
		return deserializeValues(rawValues);
	}

	public Set<TypedTuple<Object>> getDeserializeTupleValues(Collection<Tuple> rawValues) {
		return super.deserializeTupleValues(rawValues);
	}

	public Set<TypedTuple<Object>> getDeserializeTupleValues(Set<Tuple> rawValues) {
		return super.deserializeTupleValues(rawValues);
	}

	public TypedTuple<Object> getDeserializeTuple(Tuple tuple) {
		return super.deserializeTuple(tuple);
	}

	public Set<Tuple> getRawTupleValues(Set<TypedTuple<Object>> values) {
		return super.rawTupleValues(values);
	}

	public List<Object> getDeserializeValues(List<byte[]> rawValues) {
		return super.deserializeValues(rawValues);
	}

	public <T> Set<T> getDeserializeHashKeys(Set<byte[]> rawKeys) {
		return super.deserializeHashKeys(rawKeys);
	}

	public <T> List<T> getDeserializeHashValues(List<byte[]> rawValues) {
		return super.deserializeHashValues(rawValues);
	}

	public <HK, HV> Map<HK, HV> getDeserializeHashMap(@Nullable Map<byte[], byte[]> entries) {
		return super.deserializeHashMap(entries);
	}

	public String getDeserializeKey(byte[] value) {
		return super.deserializeKey(value);
	}

	public Set<String> getDeserializeKeys(Set<byte[]> keys) {
		return super.deserializeKeys(keys);
	}

	public Object getDeserializeValue(byte[] value) {
		return super.deserializeValue(value);
	}

	public String getDeserializeString(byte[] value) {
		return super.deserializeString(value);
	}

	public <HK> HK getDeserializeHashKey(byte[] value) {
		return super.deserializeHashKey(value);
	}

	public <HV> HV getDeserializeHashValue(byte[] value) {
		return super.deserializeHashValue(value);
	}

	public GeoResults<GeoLocation<Object>> getDeserializeGeoResults(GeoResults<GeoLocation<byte[]>> source) {
		return super.deserializeGeoResults(source);
	}

	// =============================Keys============================

	/**
	 * 判断key是否存在
	 *
	 * @param key 缓存key
	 * @return true 存在 false不存在
	 */
	public Boolean hasKey(String key) {
		try {
			return redisTemplate.hasKey(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 指定缓存失效时间
	 *
	 * @param key     键
	 * @param seconds 时间(秒)
	 * @return 过期是否设置成功
	 */
	public Boolean expire(String key, long seconds) {
		try {
			return redisTemplate.expire(key, seconds, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 指定缓存失效时间
	 *
	 * @param key     键
	 * @param timeout 时间
	 * @return 过期是否设置成功
	 */
	public Boolean expire(String key, Duration timeout) {
		try {
			return redisTemplate.expire(key, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 指定缓存失效时间
	 *
	 * @param key    键
	 * @param date 	 时间
	 * @return 过期是否设置成功
	 */
	public Boolean expireAt(String key, Date date) {
		try {
			return redisTemplate.expireAt(key, date);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回 key 的剩余的过期时间
	 *
	 * @param key 缓存key 不能为null
	 * @return 时间(秒) 返回0代表为永久有效
	 */
	public Long getExpire(String key) {
		try {
			return redisTemplate.getExpire(key, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回 key 的剩余的过期时间
	 *
	 * @param key 缓存key 不能为null
	 * @param unit 缓存过期时间单位
	 * @return 时间(秒) 返回0代表为永久有效
	 */
	public Long getExpire(String key, TimeUnit unit) {
		try {
			return redisTemplate.getExpire(key, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// 模糊匹配缓存中的key
	public Set<String> keys(String pattern) {
		try {
			if (Objects.isNull(pattern)) {
				return null;
			}
			Set<String> keys = this.scan(pattern);
			return keys;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除 key 的过期时间，key 将持久保持
	 *
	 * @param key 缓存key
	 * @return 持久化结果
	 */
	public Boolean persist(String key) {
		return redisTemplate.persist(key);
	}

	/**
	 * 从当前数据库中随机返回一个 key
	 *
	 * @return 随机key
	 */
	public String randomKey() {
		return redisTemplate.randomKey();
	}

	/**
	 * 修改 key 的名称
	 *
	 * @param oldKey 旧缓存key
	 * @param newKey 新缓存key
	 */
	public void rename(String oldKey, String newKey) {
		redisTemplate.rename(oldKey, newKey);
	}

	/**
	 * 仅当 newkey 不存在时，将 oldKey 改名为 newkey
	 *
	 * @param oldKey 旧缓存key
	 * @param newKey 新缓存key
	 * @return 是否修改成功
	 */
	public Boolean renameIfAbsent(String oldKey, String newKey) {
		return redisTemplate.renameIfAbsent(oldKey, newKey);
	}

	/**
	 * 返回 key 所储存的值的类型
	 *
	 * @param key 缓存key
	 * @return 缓存类型
	 */
	public DataType type(String key) {
		return redisTemplate.type(key);
	}

	// ============================String=============================

	/**
	 * 普通缓存放入
	 *
	 * @param key   键
	 * @param value 值
	 * @return true成功 false失败
	 */
	public boolean set(String key, Object value) {
		try {
			getValueOperations().set(key, value);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始
	 *
	 * @param key 缓存key
	 * @param value 缓存值
	 * @param offset 从指定位置开始覆写
	 */
	public void setRange(String key, Object value, long offset) {
		redisTemplate.opsForValue().set(key, value, offset);
	}

	/**
	 * 普通缓存放入并设置时间
	 *
	 * @param key     缓存key
	 * @param value   缓存值
	 * @param seconds 时间(秒) time要&gt;=0 如果time小于等于0 将设置无限期
	 * @return true成功 false 失败
	 */
	public boolean set(String key, Object value, long seconds) {
		try {
			if (seconds > 0) {
				getValueOperations().set(key, value, seconds, TimeUnit.SECONDS);
				return true;
			} else {
				return set(key, value);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 普通缓存放入并设置时间
	 *
	 * @param key     缓存key
	 * @param value   缓存值
	 * @param timeout 时间
	 * @return true成功 false 失败
	 */
	public boolean set(String key, Object value, Duration timeout) {
		if (Objects.isNull(timeout) || timeout.isNegative()) {
			return false;
		}
		try {
			getValueOperations().set(key, value, timeout);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public boolean setNx(String key, Object value) {
		try {
			return getValueOperations().setIfAbsent(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 1、仅可用于低并发功能，高并发严禁使用此方法
	 *
	 * @param key     并发锁
	 * @param value   锁key（务必能区别不同线程的请求）
	 * @param milliseconds 锁过期时间（单位：毫秒）
	 * @return 是否设置成功
	 */
	public boolean setNx(String key, Object value, long milliseconds) {
		try {
			return getValueOperations().setIfAbsent(key, value, Duration.ofMillis(milliseconds));
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 2、仅可用于低并发功能，高并发严禁使用此方法
	 *
	 * @param key     并发锁
	 * @param value   锁key（务必能区别不同线程的请求）
	 * @param timeout 锁过期时间
	 * @param unit    锁过期时间单位
	 * @return 是否设置成功
	 */
	public boolean setNx(String key, Object value, long timeout, TimeUnit unit) {
		try {
			return getValueOperations().setIfAbsent(key, value, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 1、仅可用于低并发功能，高并发严禁使用此方法
	 *
	 * @param key     并发锁
	 * @param value   锁key（务必能区别不同线程的请求）
	 * @param timeout 锁过期时间
	 * @return 是否设置成功
	 */
	public boolean setNx(String key, Object value, Duration timeout) {
		try {
			return getValueOperations().setIfAbsent(key, value, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @return 转换成String类型的对象
	 */
	public String getString(String key) {
		return getFor(key, TO_STRING);
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @param defaultVal 默认值
	 * @return 转换成String类型的对象
	 */
	public String getString(String key, String defaultVal) {
		String rtVal = getString(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @return 转换成Double类型的对象
	 */
	public Double getDouble(String key) {
		return getFor(key, TO_DOUBLE);
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @param defaultVal 默认值
	 * @return 转换成Double类型的对象
	 */
	public Double getDouble(String key, double defaultVal) {
		Double rtVal = getDouble(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @return 转换成Long类型的对象
	 */
	public Long getLong(String key) {
		return getFor(key, TO_LONG);
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @param defaultVal 默认值
	 * @return 转换成Long类型的对象
	 */
	public Long getLong(String key, long defaultVal) {
		Long rtVal = getLong(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @return 转换成Integer类型的对象
	 */
	public Integer getInteger(String key) {
		return getFor(key, TO_INTEGER);
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 默认值 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @param defaultVal 默认值
	 * @return 转换成Integer类型的对象
	 */
	public Integer getInteger(String key, int defaultVal) {
		Integer rtVal = getInteger(key);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 null 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @param clazz 目标对象类型 Class
	 * @param <T> 指定的类型
	 * @return 转换成目标对象类型的对象
	 */
	public <T> T getFor(String key, Class<T> clazz) {
		return getFor(key, this.toObject(clazz));
	}

	public <T> T getFor(String key, TypeReference<T> typeRef) {
		return getFor(key, this.toObject(typeRef));
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 null 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key  缓存key
	 * @param mapper 对象转换函数
	 * @param <T>  指定的类型
	 * @return 转换函数转换后的对象
	 */
	public <T> T getFor(String key, Function<Object, T> mapper) {
		Object obj = this.get(key);
		if (Objects.nonNull(obj)) {
			return mapper.apply(obj);
		}
		return null;
	}

	/**
	 * 获取指定 key 的值。如果 key 不存在，返回 null 。如果key 储存的值不是字符串类型，返回一个错误。
	 * https://www.redis.net.cn/order/3545.html
	 * @param key 缓存key
	 * @return 值
	 */
	public Object get(String key) {
		try {
			return getValueOperations().get(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取存储在指定 key 中字符串的子字符串。字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)
	 * https://www.redis.net.cn/order/3546.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 截取后的字符串
	 */
	public String getRange(String key, long start, long end) {
		try {
			return redisTemplate.opsForValue().get(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @return 转换成String类型的对象
	 */
	public String getStringAndSet(String key, Object value) {
		return getForAndSet(key, value, TO_STRING);
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @return 转换成Double类型的对象
	 */
	public Double getDoubleAndSet(String key, Double value) {
		return getForAndSet(key, value, TO_DOUBLE);
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @return 转换成Long类型的对象
	 */
	public Long getLongAndSet(String key, Long value) {
		return getForAndSet(key, value, TO_LONG);
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @return 转换成Integer类型的对象
	 */
	public Integer getIntegerAndSet(String key, Integer value) {
		return getForAndSet(key, value, TO_INTEGER);
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @param clazz 目标对象类型 Class
	 * @param <T> 指定的类型
	 * @return 转换成目标对象类型的对象
	 */
	public <T> T getForAndSet(String key, Object value, Class<T> clazz) {
		return getForAndSet(key, value, this.toObject(clazz));
	}

	public <T> T getForAndSet(String key, Object value, TypeReference<T> typeRef) {
		return getForAndSet(key, value, this.toObject(typeRef));
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @param mapper 对象转换函数
	 * @param <T> 指定的类型
	 * @return 类型转换后的对象
	 */
	public <T> T getForAndSet(String key, Object value, Function<Object, T> mapper) {
		Object obj = this.getAndSet(key, value);
		if (Objects.nonNull(obj)) {
			return mapper.apply(obj);
		}
		return null;
	}

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)
	 * https://www.redis.net.cn/order/3547.html
	 * @param key 缓存key
	 * @param value 新的值
	 * @return 缓存Key的旧值
	 */
	public Object getAndSet(String key, Object value) {
		try {
			return redisTemplate.opsForValue().getAndSet(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据key表达式获取缓存
	 * https://www.redis.net.cn/order/3549.html
	 * @param pattern 键表达式
	 * @return 值
	 */
	public List<Object> mGet(String pattern) {
		try {
			if (!StringUtils.hasText(pattern)) {
				return Lists.newArrayList();
			}
			Set<String> keys = this.keys(pattern);
			return getValueOperations().multiGet(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Double> mGetDouble(Collection keys) {
		return mGetFor(keys, TO_DOUBLE);
	}

	public List<Long> mGetLong(Collection keys) {
		return mGetFor(keys, TO_LONG);
	}

	public List<Integer> mGetInteger(Collection keys) {
		return mGetFor(keys, TO_INTEGER);
	}

	public List<String> mGetString(Collection keys) {
		return mGetFor(keys, TO_STRING);
	}

	public <T> List<T> mGetFor(Collection keys, Class<T> clazz) {
		return mGetFor(keys, this.toObject(clazz));
	}

	public <T> List<T> mGetFor(Collection keys, TypeReference<T> typeRef) {
		return mGetFor(keys, this.toObject(typeRef));
	}

	public <T> List<T> mGetFor(Collection keys, Function<Object, T> mapper) {
		List<Object> members = this.mGet(keys);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 批量获取缓存值
	 *
	 * @param keys 键集合
	 * @return 值
	 */
	public List<Object> mGet(Collection keys) {
		try {
			if(CollectionUtils.isEmpty(keys)) {
				return Lists.newArrayList();
			}
			return getValueOperations().multiGet(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Object> mGet(Collection<Object> keys, String redisPrefix) {
		try {
			if(CollectionUtils.isEmpty(keys)) {
				return Lists.newArrayList();
			}
			Collection newKeys = keys.stream().map(key -> RedisKey.getKeyStr(redisPrefix, key.toString())).collect(Collectors.toList());
			return getValueOperations().multiGet(newKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key   键
	 * @param delta 要增加几(&gt;=0)
	 * @return 增加指定数值后的值
	 */
	public Long incr(String key, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getValueOperations().increment(key, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key     键
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的值
	 */
	public Long incr(String key, long delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getValueOperations().increment(key, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long incr(String key, long delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getValueOperations().increment(key, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key   键
	 * @param delta 要增加几(&gt;=0)
	 * @return 增加指定数值后的值
	 */
	public Double incr(String key, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getValueOperations().increment(key, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递增
	 *
	 * @param key     键
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的值
	 */
	public Double incr(String key, double delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getValueOperations().increment(key, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Double incr(String key, double delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getValueOperations().increment(key, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key   键
	 * @param delta 要减少几(&gt;=0)
	 * @return 减少指定数值后的值
	 */
	public Long decr(String key, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getValueOperations().increment(key, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key     键
	 * @param delta   要减少几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 减少指定数值后的值
	 */
	public Long decr(String key, long delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Long increment = getValueOperations().increment(key, -delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long decr(String key, long delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Long increment = getValueOperations().increment(key, -delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key   键
	 * @param delta 要减少几(&gt;=0)
	 * @return 减少指定数值后的值
	 */
	public Double decr(String key, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getValueOperations().increment(key, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 递减
	 *
	 * @param key     键
	 * @param delta   要减少几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 减少指定数值后的值
	 */
	public Double decr(String key, double delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Double increment = getValueOperations().increment(key, -delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Double decr(String key, double delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			Double increment = getValueOperations().increment(key, -delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 删除缓存
	 *
	 * @param keys 可以传一个值 或多个
	 */
	public Long del(String... keys) {
		try {
			if (keys != null && keys.length > 0) {
				if (keys.length == 1) {
					redisTemplate.delete(keys[0]);
					return 1L;
				} else {
					return redisTemplate.delete(Stream.of(keys).collect(Collectors.toList()));
				}
			}
			return 0L;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long delPattern(String pattern) {
		try {
			Set<String> keys = this.keys(pattern);
			if(CollectionUtils.isEmpty(keys)){
				return 0L;
			}
			return redisTemplate.delete(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * scan 实现
	 *
	 * @param pattern  表达式
	 * @param count 数量限制
	 * @return 扫描结果
	 */
	public Set<String> scan(String pattern, long count) {
		ScanOptions options = ScanOptions.scanOptions().count(count).match(pattern).build();
		return this.scan(options);
	}

	public Set<String> scan(String pattern) {
		ScanOptions options = ScanOptions.scanOptions().match(pattern).build();
		return this.scan(options);
	}

	public Set<String> scan(ScanOptions options) {
		return this.redisTemplate.execute((RedisConnection redisConnection) -> {
			try (Cursor<byte[]> cursor = redisConnection.scan(options)) {
				Set<String> keysTmp = new HashSet<>();
				while (cursor.hasNext()) {
					keysTmp.add(deserializeString(cursor.next()));
				}
				return keysTmp;
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RedisOperationException(e.getMessage());
			}
		});
	}

	// ===============================List=================================

	/**
	 * 获取list缓存的内容
	 *
	 * @param key   键
	 * @param start 开始
	 * @param end   结束 0 到 -1代表所有值
	 * @return list 集合
	 */
	public List<Object> lRange(String key, long start, long end) {
		try {
			return redisTemplate.opsForList().range(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<String> lRangeString(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_STRING);
	}

	public List<Double> lRangeDouble(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_DOUBLE);
	}

	public List<Long> lRangeLong(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_LONG);
	}

	public List<Integer> lRangeInteger(String key, long start, long end) {
		return lRangeFor(key, start, end, TO_INTEGER);
	}

	/**
	 * 获取指定类型的list缓存
	 *
	 * @param key   键
	 * @param start 开始下标
	 * @param end   结束下标, 0 到 -1代表所有值
	 * @param clazz 指定的类型
	 * @param <T>   指定的类型
	 * @return 类型转换后的集合
	 */
	public <T> List<T> lRangeFor(String key, long start, long end, Class<T> clazz) {
		return lRangeFor(key, start, end, this.toObject(clazz));
	}

	public <T> List<T> lRangeFor(String key, long start, long end, TypeReference<T> typeRef) {
		return lRangeFor(key, start, end, this.toObject(typeRef));
	}

	/**
	 * 获取list缓存的，并指定转换器
	 * @param key   键
	 * @param start 开始下标
	 * @param end   结束下标, 0 到 -1代表所有值
	 * @param mapper 对象转换函数
	 * @param <T>   指定的类型
	 * @return 类型转换后的集合
	 */
	public <T> List<T> lRangeFor(String key, long start, long end, Function<Object, T> mapper) {
		List<Object> members = this.lRange(key, start, end);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	/**
	 * 通过索引 获取list中的值
	 *
	 * @param key   键
	 * @param index 索引 index&gt;=0时， 0 表头，1 第二个元素，依次类推；index&lt;0时，-1，表尾，-2倒数第二个元素，依次类推
	 * @return 所以所在位置元素
	 */
	public Object lIndex(String key, long index) {
		try {
			return redisTemplate.opsForList().index(key, index);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public String lIndexString(String key, long index) {
		return lIndexFor(key, index, TO_STRING);
	}

	public String glIndexString(String key, long index, String defaultVal) {
		String rtVal = lIndexString(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public Double lIndexDouble(String key, long index) {
		return lIndexFor(key, index, TO_DOUBLE);
	}

	public Double lIndexDouble(String key, long index, double defaultVal) {
		Double rtVal = lIndexDouble(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public Long lIndexLong(String key, long index) {
		return lIndexFor(key, index, TO_LONG);
	}

	public Long lIndexLong(String key, long index, long defaultVal) {
		Long rtVal = lIndexLong(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public Integer lIndexInteger(String key, long index) {
		return lIndexFor(key, index, TO_INTEGER);
	}

	public Integer lIndexInteger(String key, long index, int defaultVal) {
		Integer rtVal = lIndexInteger(key, index);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public <T> T lIndexFor(String key, long index, Function<Object, T> mapper) {
		Object member = lIndex(key, index);
		if (Objects.nonNull(member)) {
			return mapper.apply(member);
		}
		return null;
	}

	public <V> Long lLeftPushDistinct(String key, V value) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = rawValue(value);
				redisConnection.lRem(rawKey, 0, rawValue);
				redisConnection.lPush(rawKey, rawValue);
				return null;
			}, this.valueSerializer());
			return (Long) result.get(1);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPush(String key, V value) {
		return this.lLeftPush(key, value, 0);
	}

	public <V> Long lLeftPush(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lLeftPushAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = redisTemplate.opsForList().leftPush(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPush(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lLeftPushAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = redisTemplate.opsForList().leftPush(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushAll(String key, Collection<V> values) {
		try {
			Long rt = redisTemplate.opsForList().leftPushAll(key, values.toArray());
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushAll(String key, Collection<V> values, long seconds) {
		try {
			Long rt = redisTemplate.opsForList().leftPushAll(key, values.toArray());
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushAll(String key, Collection<V> values, Duration timeout) {
		try {
			Long rt = redisTemplate.opsForList().leftPushAll(key, values.toArray());
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushx(String key, V value) {
		return this.lLeftPushx(key, value, 0);
	}

	public <V> Long lLeftPushx(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lLeftPushxAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = redisTemplate.opsForList().leftPushIfPresent(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushx(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lLeftPushxAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = redisTemplate.opsForList().leftPushIfPresent(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushxAll(String key, Collection<V> values, long seconds) {
		try {
			long rt = values.stream().map(value -> redisTemplate.opsForList().leftPushIfPresent(key, value)).count();
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lLeftPushxAll(String key, Collection<V> values, Duration timeout) {
		try {
			long rt = values.stream().map(value -> redisTemplate.opsForList().leftPushIfPresent(key, value)).count();
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lLeftPop(String key) {
		try {
			return redisTemplate.opsForList().leftPop(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Object lLeftPopAndLrem(String key) {
		try {
			return redisTemplate.execute((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = redisConnection.lPop(rawKey);
				redisConnection.lRem(rawKey, 0, rawValue);
				return deserializeValue(rawValue);
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lLeftPop(String key, long timeout, TimeUnit unit) {
		try {
			return redisTemplate.opsForList().leftPop(key, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lLeftPop(String key, Duration timeout) {
		try {
			return redisTemplate.opsForList().leftPop(key, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从list左侧取count个元素并移除已经去除的元素
	 *
	 * @param key 缓存key
	 * @param count 去除元素的个数
	 * @return 被移除元素列表
	 */
	public List<Object> lLeftPop(String key, Integer count) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				redisConnection.lRange(rawKey, 0, count - 1);
				redisConnection.lTrim(rawKey, count, -1);
				return null;
			}, this.valueSerializer());
			return (List<Object>) result.get(0);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <T> List<T> lLeftPop(String key, Integer count, Class<T> clazz) {
		try {
			List<Object> range = this.lLeftPop(key, count);
			List<T> result = range.stream().map(this.toObject(clazz)).collect(Collectors.toList());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <T> List<T> lLeftPop(String key, Integer count, TypeReference<T> typeRef) {
		try {
			List<Object> range = this.lLeftPop(key, count);
			List<T> result = range.stream().map(this.toObject(typeRef)).collect(Collectors.toList());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushDistinct(String key, V value) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = rawValue(value);
				redisConnection.lRem(rawKey, 0, rawValue);
				redisConnection.rPush(rawKey, rawValue);
				return null;
			}, this.valueSerializer());
			return (Long) result.get(1);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key   键
	 * @param value 值
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPush(String key, V value) {
		return this.lRightPush(key, value, 0);
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key     键
	 * @param value   值
	 * @param seconds 时间(秒)
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPush(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lRightPushAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = redisTemplate.opsForList().rightPush(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPush(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lRightPushAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = redisTemplate.opsForList().rightPush(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushAll(String key, Collection<V> values) {
		try {
			return redisTemplate.opsForList().rightPushAll(key, values.toArray());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushAll(String key, Collection<V> values, long seconds) {
		try {
			Long rt = redisTemplate.opsForList().rightPushAll(key, values.toArray());
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushAll(String key, Collection<V> values, Duration timeout) {
		try {
			Long rt = redisTemplate.opsForList().rightPushAll(key, values.toArray());
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key   键
	 * @param value 值
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPushx(String key, V value) {
		return this.lRightPushx(key, value, 0);
	}

	/**
	 * 将对象放入缓存
	 *
	 * @param key     键
	 * @param value   值
	 * @param seconds 时间(秒)
	 * @param <V>   值的类型
	 * @return 成功添加的个数
	 */
	public <V> Long lRightPushx(String key, V value, long seconds) {
		if (value instanceof Collection) {
			return lRightPushxAll(key, (Collection) value, seconds);
		}
		try {
			Long rt = redisTemplate.opsForList().rightPushIfPresent(key, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushx(String key, V value, Duration timeout) {
		if (value instanceof Collection) {
			return lRightPushxAll(key, (Collection) value, timeout);
		}
		try {
			Long rt = redisTemplate.opsForList().rightPushIfPresent(key, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushxAll(String key, Collection<V> values, long seconds) {
		try {
			long rt = values.stream().map(value -> redisTemplate.opsForList().rightPushIfPresent(key, value)).count();
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Long lRightPushxAll(String key, Collection<V> values, Duration timeout) {
		try {
			long rt = values.stream().map(value -> redisTemplate.opsForList().rightPushIfPresent(key, value)).count();
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lRightPop(String key) {
		try {
			return redisTemplate.opsForList().rightPop(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> Object lRightPopAndLrem(String key) {
		try {
			return redisTemplate.execute((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				byte[] rawValue = redisConnection.rPop(rawKey);
				redisConnection.lRem(rawKey, 0, rawValue);
				return deserializeValue(rawValue);
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lRightPop(String key, long timeout, TimeUnit unit) {
		try {
			return redisTemplate.opsForList().rightPop(key, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lRightPop(String key, Duration timeout) {
		try {
			return redisTemplate.opsForList().rightPop(key, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 从list右侧取count个元素并移除已经去除的元素
	 *	1、Redis Ltrim 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
	 *  2、下标 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * @param key 缓存key
	 * @param count 移除元素个数
	 * @return 右侧移除的元素集合
	 */
	public List<Object> lRightPop(String key, Integer count) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection redisConnection) -> {
				byte[] rawKey = rawKey(key);
				redisConnection.lRange(rawKey, -(count - 1), -1);
				redisConnection.lTrim(rawKey, 0, -(count - 1));
				return null;
			}, this.valueSerializer());
			return (List<Object>) result.get(0);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lRightPopAndLeftPush(String sourceKey, String destinationKey) {
		try {
			return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lRightPopAndLeftPush(String sourceKey, String destinationKey, long timeout, TimeUnit unit) {
		try {
			return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout, unit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Object lRightPopAndLeftPush(String sourceKey, String destinationKey, Duration timeout) {
		try {
			return redisTemplate.opsForList().rightPopAndLeftPush(sourceKey, destinationKey, timeout);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据索引修改list中的某条数据
	 *
	 * @param key   键
	 * @param index 索引
	 * @param value 值
	 * @return 元素是否设置成功
	 */
	public boolean lSet(String key, long index, Object value) {
		try {
			redisTemplate.opsForList().set(key, index, value);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取list缓存的长度
	 *
	 * @param key 缓存key
	 * @return list集合的元素个数
	 */
	public long lSize(String key) {
		try {
			return redisTemplate.opsForList().size(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public boolean lTrim(String key, long start, long end) {
		try {
			redisTemplate.opsForList().trim(key, start, end);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除N个值为value
	 *
	 * @param key   键
	 * @param count 移除多少个
	 * @param value 值
	 * @return 移除的个数
	 */
	public Long lRem(String key, long count, Object value) {
		try {
			return redisTemplate.opsForList().remove(key, count, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ================================Hash=================================

	/**
	 * hash递减
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要减少记(小于0)
	 * @return 减少指定数值后的结果
	 */
	public Long hDecr(String key, Object hashKey, int delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getHashOperations().increment(key, hashKey, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递减
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要减少记(&gt;=0)
	 * @return 减少指定数值后的结果
	 */
	public Long hDecr(String key, Object hashKey, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getHashOperations().increment(key, hashKey, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递减
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要减少记(&gt;=0)
	 * @return 减少指定数值后的结果
	 */
	public Double hDecr(String key, Object hashKey, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递减因子必须>=0");
		}
		try {
			return getHashOperations().increment(key, hashKey, -delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 删除hash表中的值
	 *
	 * @param key      键 不能为null
	 * @param hashKeys 项 可以使多个 不能为null
	 */
	public void hDel(String key, Object... hashKeys) {
		try {
			getHashOperations().delete(key, hashKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Hash删除: hscan + hdel
	 *
	 * @param bigHashKey hash key
	 */
	public void hDel(String bigHashKey) {
		try {
			this.hScan(bigHashKey, (entry) -> {
				this.hDel(bigHashKey, entry.getKey());
			});
			this.del(bigHashKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 判断hash表中是否有该项的值
	 *
	 * @param key     键 不能为null
	 * @param hashKey 项 不能为null
	 * @return true 存在 false不存在
	 */
	public boolean hHasKey(String key, Object hashKey) {
		try {
			return getHashOperations().hasKey(key, hashKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取hashKey对应的指定键值
	 *
	 * @param key     键
	 * @param hashKey hash键
	 * @return 对应的键值
	 */
	public Object hGet(String key, Object hashKey) {
		try {
			return getHashOperations().get(key, hashKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> V hGet(String key, Object hashKey, V defaultVal) {
		try {
			Object rtVal = getHashOperations().get(key, hashKey);
			return Objects.nonNull(rtVal) ? (V) rtVal : defaultVal;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public String hGetString(String key, Object hashKey) {
		return hGetFor(key, hashKey, TO_STRING);
	}

	public String hGetString(String key, Object hashKey, String defaultVal) {
		String rtVal = hGetString(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public Double hGetDouble(String key, Object hashKey) {
		return hGetFor(key, hashKey, TO_DOUBLE);
	}

	public Double hGetDouble(String key, Object hashKey, double defaultVal) {
		Double rtVal = hGetDouble(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public Long hGetLong(String key, Object hashKey) {
		return hGetFor(key, hashKey, TO_LONG);
	}

	public Long hGetLong(String key, Object hashKey, long defaultVal) {
		Long rtVal = hGetLong(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public Integer hGetInteger(String key, Object hashKey) {
		return hGetFor(key, hashKey, TO_INTEGER);
	}

	public Integer hGetInteger(String key, Object hashKey, int defaultVal) {
		Integer rtVal = hGetInteger(key, hashKey);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	public <T> T hGetFor(String key, Object hashKey, Class<T> clazz) {
		return hGetFor(key, hashKey, this.toObject(clazz));
	}

	public <T> T hGetFor(String key, Object hashKey, TypeReference<T> typeRef) {
		return hGetFor(key, hashKey, this.toObject(typeRef));
	}

	public <T> T hGetFor(String key, Object hashKey, Function<Object, T> mapper) {
		Object rt = this.hGet(key, hashKey);
		return Objects.nonNull(rt) ? mapper.apply(rt) : null;
	}

	public List<String> hGetString(Collection<Object> keys, Object hashKey) {
		return hGetFor(keys, hashKey, TO_STRING);
	}

	public List<Double> hGetDouble(Collection<Object> keys, Object hashKey) {
		return hGetFor(keys, hashKey, TO_DOUBLE);
	}

	public List<Long> hGetLong(Collection<Object> keys, Object hashKey) {
		return hGetFor(keys, hashKey, TO_LONG);
	}

	public List<Integer> hGetInteger(Collection<Object> keys, Object hashKey) {
		return hGetFor(keys, hashKey, TO_INTEGER);
	}

	public <T> List<T> hGetFor(Collection<Object> keys, Object hashKey, Class<T> clazz) {
		return hGetFor(keys, hashKey, this.toObject(clazz));
	}

	public <T> List<T> hGetFor(Collection<Object> keys, Object hashKey, TypeReference<T> typeRef) {
		return hGetFor(keys, hashKey, this.toObject(typeRef));
	}

	public <T> List<T> hGetFor(Collection<Object> keys, Object hashKey, Function<Object, T> mapper) {
		List<Object> members = this.hGet(keys, hashKey);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	public List<Object> hGet(Collection<Object> keys, Object hashKey) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					connection.hGet(rawKey(key), rawHashKey(hashKey));
				});
				return null;
			}, this.valueSerializer());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Object> hGet(Collection<Object> keys, String redisPrefix, Object hashKey) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(RedisKey.getKeyStr(redisPrefix, String.valueOf(key)));
					connection.hGet(rawKey, rawHashKey(hashKey));
				});
				return null;
			}, this.valueSerializer());
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <HV> HV hmGetFor(String key, Class<HV> clazz) {
		Map<Object, Object> map = this.hmGet(key);
		if (Objects.nonNull(map)) {
			return this.toObject(clazz).apply(map);

		}
		return null;
	}

	public <HV> HV hmGetFor(String key, TypeReference<HV> typeRef) {
		Map<Object, Object> map = this.hmGet(key);
		if (Objects.nonNull(map)) {
			return this.toObject(typeRef).apply(map);

		}
		return null;
	}

	public <HV> Map<String, HV> hmGetForString(String key, Function<Object, HV> valueMapper) {
		return this.hmGetFor(key, TO_STRING, valueMapper);
	}

	public <HV> Map<String, HV> hmGetForString(String key, Class<HV> clazz) {
		return this.hmGetFor(key, TO_STRING, clazz);
	}

	public <HV> Map<String, HV> hmGetForString(String key, TypeReference<HV> typeRef) {
		return this.hmGetFor(key, TO_STRING, typeRef);
	}

	public <HV> Map<Integer, HV> hmGetForInteger(String key, Function<Object, HV> valueMapper) {
		return this.hmGetFor(key, TO_INTEGER, valueMapper);
	}

	public <HV> Map<Integer, HV> hmGetForInteger(String key, Class<HV> clazz) {
		return this.hmGetFor(key, TO_INTEGER, clazz);
	}

	public <HV> Map<Integer, HV> hmGetForInteger(String key, TypeReference<HV> typeRef) {
		return this.hmGetFor(key, TO_INTEGER, typeRef);
	}

	public <HV> Map<Long, HV> hmGetForLong(String key, Function<Object, HV> valueMapper) {
		return this.hmGetFor(key, TO_LONG, valueMapper);
	}

	public <HV> Map<Long, HV> hmGetForLong(String key, Class<HV> clazz) {
		return this.hmGetFor(key, TO_LONG, clazz);
	}

	public <HV> Map<Long, HV> hmGetForLong(String key, TypeReference<HV> typeRef) {
		return this.hmGetFor(key, TO_LONG, typeRef);
	}

	public <HV> Map<Double, HV> hmGetForDouble(String key, Function<Object, HV> valueMapper) {
		return this.hmGetFor(key, TO_DOUBLE, valueMapper);
	}

	public <HV> Map<Double, HV> hmGetForDouble(String key, Class<HV> clazz) {
		return this.hmGetFor(key, TO_DOUBLE, clazz);
	}

	public <HV> Map<Double, HV> hmGetForDouble(String key, TypeReference<HV> typeRef) {
		return this.hmGetFor(key, TO_DOUBLE, typeRef);
	}

	public Map<String, String> hmGetForString(String key) {
		return this.hmGetFor(key, TO_STRING, TO_STRING);
	}

	public Map<Integer, Integer> hmGetForInteger(String key) {
		return this.hmGetFor(key, TO_INTEGER, TO_INTEGER);
	}

	public Map<Long, Long> hmGetForLong(String key) {
		return this.hmGetFor(key, TO_LONG, TO_LONG);
	}

	public Map<Double, Double> hmGetForDouble(String key) {
		return this.hmGetFor(key, TO_DOUBLE, TO_DOUBLE);
	}

	public <HK, HV> Map<HK, HV> hmGetFor(String key, Function<Object, HK> keyMapper, Function<Object, HV> valueMapper) {
		Map<Object, Object> map = this.hmGet(key);
		if (Objects.nonNull(map)) {
			return map.entrySet().stream().collect(Collectors.toMap(entry -> keyMapper.apply(entry.getKey()), entry -> valueMapper.apply(entry.getValue())));
		}
		return Collections.emptyMap();
	}

	public <HK, HV> Map<HK, HV> hmGetFor(String key, Function<Object, HK> keyMapper, Class<HV> clazz) {
		Map<Object, Object> map = this.hmGet(key);
		if (Objects.nonNull(map)) {
			return map.entrySet().stream().collect(Collectors.toMap(entry -> keyMapper.apply(entry.getKey()), entry -> this.toObject(clazz).apply(entry.getValue())));
		}
		return Collections.emptyMap();
	}

	public <HK, HV> Map<HK, HV> hmGetFor(String key, Function<Object, HK> keyMapper, TypeReference<HV> typeRef) {
		Map<Object, Object> map = this.hmGet(key);
		if (Objects.nonNull(map)) {
			return map.entrySet().stream().collect(Collectors.toMap(entry -> keyMapper.apply(entry.getKey()), entry -> this.toObject(typeRef).apply(entry.getValue())));
		}
		return Collections.emptyMap();
	}

	public List<Map<Object, Object>> hmGet(Collection<String> keys) {
		if (CollectionUtils.isEmpty(keys)) {
			return Lists.newArrayList();
		}
		return keys.parallelStream().map(key -> {
			return this.hmGet(key);
		}).collect(Collectors.toList());
	}

	/**
	 * 获取hashKey对应的所有键值
	 *
	 * @param key 缓存key
	 * @return 对应的多个键值
	 */
	public Map<Object, Object> hmGet(String key) {
		try {
			HashOperations<String, Object, Object> opsForHash = getHashOperations();
			return opsForHash.entries(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Map<Object, Object>> hmGet(Collection<String> keys, String redisPrefix) {
		if (CollectionUtils.isEmpty(keys)) {
			return Lists.newArrayList();
		}
		return keys.parallelStream().map(key -> {
			return this.hmGet(RedisKey.getKeyStr(redisPrefix, key));
		}).collect(Collectors.toList());
	}

	public List<Object> hmGet(String key, Collection<Object> hashKeys) {
		try {
			return getHashOperations().multiGet(key, hashKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Map<Object, Object> hmMultiGet(String key, Collection<Object> hashKeys) {
		try {
			List<Object> result = getHashOperations().multiGet(key, hashKeys);
			Map<Object, Object> ans = new HashMap<>(hashKeys.size());
			int index = 0;
			for (Object hashKey : hashKeys) {
				ans.put(hashKey.toString(), result.get(index));
				index++;
			}
			return ans;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Map<Object, Object>> hmMultiGet(Collection<String> keys, Collection<Object> hashKeys) {
		if (CollectionUtils.isEmpty(keys) || CollectionUtils.isEmpty(hashKeys)) {
			return Lists.newArrayList();
		}
		return keys.parallelStream().map(key -> {
			return this.hmMultiGet(key, hashKeys);
		}).collect(Collectors.toList());
	}

	public Map<String, Map<Object, Object>> hmMultiGet(Collection<String> keys, String identityHashKey, Collection<Object> hashKeys) {
		if (CollectionUtils.isEmpty(keys) || CollectionUtils.isEmpty(hashKeys)) {
			return Maps.newHashMap();
		}
		return keys.parallelStream().map(key -> {
			return this.hmMultiGet(key, hashKeys);
		}).collect(Collectors.toMap(kv -> MapUtils.getString(kv, identityHashKey), Function.identity()));
	}


	public List<Map<Object, Object>> hGetAll(Collection<String> keys) {
		return this.hGetAllFor(keys, entry -> (Map<Object, Object>) entry);
	}

	public <T> List<T> hGetAllFor(Collection<String> keys, Class<T> clazz) {
		return this.hGetAllFor(keys, this.toObject(clazz));
	}

	public <T> List<T> hGetAllFor(Collection<String> keys, TypeReference<T> typeRef) {
		return this.hGetAllFor(keys, this.toObject(typeRef));
	}

	public <T> List<T> hGetAllFor(Collection<String> keys, Function<Object, T> objectMapper) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(key);
					connection.hGetAll(rawKey);
				});
				return null;
			}, this.valueSerializer());
			return result.stream().filter(Objects::nonNull)
					.map(objectMapper)
					.collect(Collectors.toList());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Map<Object, Object>> hGetAll(Collection<Object> keys, String redisPrefix) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection connection) -> {
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(RedisKey.getKeyStr(redisPrefix, String.valueOf(key)));
					connection.hGetAll(rawKey);
				});
				return null;
			}, this.valueSerializer());
			return result.stream().map(mapper -> (Map<Object, Object>) mapper).collect(Collectors.toList());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<String> hMultiGetString(Collection<Object> keys, Object hashKey) {
		return hMultiGetFor(keys, hashKey, TO_STRING);
	}

	public List<Double> hMultiGetDouble(Collection<Object> keys, Object hashKey) {
		return hMultiGetFor(keys, hashKey, TO_DOUBLE);
	}

	public List<Long> hMultiGetLong(Collection<Object> keys, Object hashKey) {
		return hMultiGetFor(keys, hashKey, TO_LONG);
	}

	public List<Integer> hMultiGetInteger(Collection<Object> keys, Object hashKey) {
		return hMultiGetFor(keys, hashKey, TO_INTEGER);
	}

	public <T> List<T> hMultiGetFor(Collection<Object> keys, Object hashKey, Class<T> clazz) {
		return hMultiGetFor(keys, hashKey, this.toObject(clazz));
	}

	public <T> List<T> hMultiGetFor(Collection<Object> keys, Object hashKey, TypeReference<T> typeRef) {
		return hMultiGetFor(keys, hashKey, this.toObject(typeRef));
	}

	public <T> List<T> hMultiGetFor(Collection<Object> keys, Object hashKey, Function<Object, T> mapper) {
		List<Object> members = this.hMultiGet(keys, hashKey);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	public List<Object> hMultiGet(Collection<Object> keys, Object hashKey) {
		try {
			List<Object> result = redisTemplate.executePipelined((RedisConnection connection) -> {
				byte[] rawHashKey = rawHashKey(hashKey);
				keys.stream().forEach(key -> {
					byte[] rawKey = rawKey(String.valueOf(key));
					connection.hGet(rawKey, rawHashKey);
				});
				return null;
			}, this.valueSerializer());
			return result.stream().collect(Collectors.toList());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}


	/**
	 * HashSet
	 *
	 * @param key 缓存key
	 * @param map 对应多个键值
	 * @return true 成功 false 失败
	 */
	public <K, V> boolean hmSet(String key, Map<K, V> map) {
		try {
			getHashOperations().putAll(key, map);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * HashSet 并设置时间
	 *
	 * @param key     键
	 * @param map     对应多个键值
	 * @param seconds 时间(秒)
	 * @return true成功 false失败
	 */
	public <K, V> boolean hmSet(String key, Map<K, V> map, long seconds) {
		try {
			getHashOperations().putAll(key, map);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <K, V> boolean hmSet(String key, Map<K, V> map, Duration timeout) {
		try {
			getHashOperations().putAll(key, map);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void hScan(String bigHashKey, Consumer<Entry<Object,Object>> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).build();
		this.hScan(bigHashKey, options).forEachRemaining(consumer);
	}

	public void hScan(String bigHashKey, String pattern, Consumer<Entry<Object,Object>> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build();
		this.hScan(bigHashKey, options).forEachRemaining(consumer);
	}

	public void hScan(String bigHashKey, ScanOptions options, Consumer<Entry<Object,Object>> consumer) {
		this.hScan(bigHashKey, options).forEachRemaining(consumer);
	}

	public Cursor<Entry<Object, Object>> hScan(String bigHashKey, ScanOptions options) {
		return getHashOperations().scan(bigHashKey, options);
	}

	/**
	 * 向一张hash表中放入数据,如果不存在将创建
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param value   值
	 * @return true 成功 false失败
	 */
	public boolean hSet(String key, Object hashKey, Object value) {
		try {
			getHashOperations().put(key, hashKey, value);
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public boolean hSet(Collection<String> keys, Object hashKey, Object value) {
		if (CollectionUtils.isEmpty(keys) || Objects.isNull(hashKey)) {
			return false;
		}
		try {
			redisTemplate.executePipelined((RedisConnection connection) -> {
				byte[] rawHashKey = rawHashKey(hashKey);
				byte[] rawHashValue = rawHashValue(value);
				for (String key : keys) {
					byte[] rawKey = rawKey(key);
					connection.hSet(rawKey, rawHashKey, rawHashValue);
				}
				return null;
			});
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向一张hash表中放入数据,如果不存在将创建
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param value   值
	 * @param seconds    时间(秒) 注意:如果已存在的hash表有时间,这里将会替换原有的时间
	 * @return true 成功 false失败
	 */
	public boolean hSet(String key, Object hashKey, Object value, long seconds) {
		try {
			getHashOperations().put(key, hashKey, value);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public boolean hSet(String key, Object hashKey, Object value, Duration timeout) {
		try {
			getHashOperations().put(key, hashKey, value);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return true;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public boolean hSetNX(String key, Object hashKey, Object value) {
		try {
			return getHashOperations().putIfAbsent(key, hashKey, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取所有哈希表中的字段
	 *
	 * @param key 缓存key
	 * @return  哈希缓存的所有key
	 */
	public Set<Object> hKeys(String key) {
		try {
			return getHashOperations().keys(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash的大小
	 *
	 * @param key 缓存key
	 * @return  hash的大小
	 */
	public Long hSize(String key) {
		try {
			return getHashOperations().size(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取哈希表中所有值
	 *
	 * @param key 缓存key
	 * @return 哈希表中所有值
	 */
	public List<Object> hValues(String key) {
		try {
			return getHashOperations().values(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @return 增加指定数值后的结果
	 */
	public Long hIncr(String key, Object hashKey, int delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getHashOperations().increment(key, hashKey, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的结果
	 */
	public Long hIncr(String key, Object hashKey, int delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getHashOperations().increment(key, hashKey, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long hIncr(String key, Object hashKey, int delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getHashOperations().increment(key, hashKey, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @return 增加指定数值后的新数值
	 */
	public Long hIncr(String key, Object hashKey, long delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getHashOperations().increment(key, hashKey, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @param seconds 过期时长（秒）
	 * @return 增加指定数值后的新数值
	 */
	public Long hIncr(String key, Object hashKey, long delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getHashOperations().increment(key, hashKey, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @param timeout 过期时长（秒）
	 * @return 增加指定数值后的新数值
	 */
	public Long hIncr(String key, Object hashKey, long delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Long increment = getHashOperations().increment(key, hashKey, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * hash递增 如果不存在,就会创建一个 并把新增后的值返回
	 *
	 * @param key     键
	 * @param hashKey 项
	 * @param delta   要增加几(&gt;=0)
	 * @return 增加指定数值后的新数值
	 */
	public Double hIncr(String key, Object hashKey, double delta) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			return getHashOperations().increment(key, hashKey, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Double hIncr(String key, Object hashKey, double delta, long seconds) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getHashOperations().increment(key, hashKey, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Double hIncr(String key, Object hashKey, double delta, Duration timeout) {
		if (delta < 0) {
			throw new RedisOperationException("递增因子必须>=0");
		}
		try {
			Double increment = getHashOperations().increment(key, hashKey, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return increment;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	protected HashOperations<String, Object, Object> getHashOperations() {
		return redisTemplate.opsForHash();
	}

	// ============================Set=============================

	/**
	 * 将数据放入set缓存
	 *
	 * @param key    键
	 * @param values 值 可以是多个
	 * @return 成功个数
	 */
	public Long sAdd(String key, Object... values) {
		try {
			return getSetOperations().add(key, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sAddAndExpire(String key, long seconds, Object... values) {
		try {
			Long rt = getSetOperations().add(key, values);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sAddAndExpire(String key, Duration timeout, Object... values) {
		try {
			Long rt = getSetOperations().add(key, values);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return rt;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Set删除: sscan + srem
	 *
	 * @param bigSetKey 键
	 * @return 批量删除结果
	 */
	public Boolean sDel(String bigSetKey) {
		try {
			this.sScan(bigSetKey, (value) -> {
				getSetOperations().remove(bigSetKey, deserializeValue(value));
			});
			return redisTemplate.delete(bigSetKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据key获取Set中的所有值
	 *
	 * @param key 缓存key
	 * @return Set 集合的所有元素
	 */
	public Set<Object> sGet(String key) {
		try {
			return getSetOperations().members(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Set<String> sGetString(String key) {
		return sGetFor(key, TO_STRING);
	}

	public Set<Double> sGetDouble(String key) {
		return sGetFor(key, TO_DOUBLE);
	}

	public Set<Long> sGetLong(String key) {
		return sGetFor(key, TO_LONG);
	}

	public Set<Integer> sGetInteger(String key) {
		return sGetFor(key, TO_INTEGER);
	}

	public <T> Set<T> sGetFor(String key, Class<T> clazz) {
		return sGetFor(key, this.toObject(clazz));
	}

	public <T> Set<T> sGetFor(String key, TypeReference<T> typeRef) {
		return sGetFor(key, this.toObject(typeRef));
	}

	public <T> Set<T> sGetFor(String key, Function<Object, T> mapper) {
		Set<Object> members = this.sGet(key);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 获取两个key的不同value
	 *
	 * @param key      键
	 * @param otherKey 键
	 * @return 返回key中和otherKey的不同数据
	 */
	public Set<Object> sDiff(String key, String otherKey) {
		try {
			return getSetOperations().difference(key, otherKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取两个key的不同数据，存储到destKey中
	 *
	 * @param key      键
	 * @param otherKey 键
	 * @param destKey  键
	 * @return 返回成功数据
	 */
	public Long sDiffAndStore(String key, String otherKey, String destKey) {
		try {
			return getSetOperations().differenceAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取key和keys的不同数据，存储到destKey中
	 *
	 * @param key     键
	 * @param keys    键集合
	 * @param destKey 键
	 * @return 返回成功数据
	 */
	public Long sDiffAndStore(String key, Collection<String> keys, String destKey) {
		try {
			return getSetOperations().differenceAndStore(key, keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取多个keys的不同数据，存储到destKey中
	 *
	 * @param keys    键集合
	 * @param destKey 键
	 * @return 返回成功数据
	 */
	public Long sDiffAndStore(Collection<String> keys, String destKey) {
		try {
			return getSetOperations().differenceAndStore(keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 根据value从一个set中查询,是否存在
	 *
	 * @param key   键
	 * @param value 值
	 * @return true 存在 false不存在
	 */
	public boolean sHasKey(String key, Object value) {
		try {
			return getSetOperations().isMember(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Set<Object> sIntersect(String key, String otherKey) {
		try {
			return getSetOperations().intersect(key, otherKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Set<Object> sIntersect(String key, Collection<String> otherKeys) {
		try {
			return getSetOperations().intersect(key, otherKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Set<Object> sIntersect(Collection<String> otherKeys) {
		try {
			return getSetOperations().intersect(otherKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sIntersectAndStore(String key, String otherKey, String destKey) {
		try {
			return getSetOperations().intersectAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sIntersectAndStore(String key, Collection<String> otherKeys, String destKey) {
		try {
			return getSetOperations().intersectAndStore(key, otherKeys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sIntersectAndStore(Collection<String> otherKeys, String destKey) {
		try {
			return getSetOperations().intersectAndStore(otherKeys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 随机获取指定数量的元素,同一个元素可能会选中两次
	 *
	 * @param key 缓存key
	 * @param count 随机获取元素数量
	 * @return 随机获取的元素集合
	 */
	public List<String> sRandomString(String key, long count) {
		return sRandomFor(key, count, TO_STRING);
	}

	public List<Double> sRandomDouble(String key, long count) {
		return sRandomFor(key, count, TO_DOUBLE);
	}

	public List<Long> sRandomLong(String key, long count) {
		return sRandomFor(key, count, TO_LONG);
	}

	public List<Integer> sRandomInteger(String key, long count) {
		return sRandomFor(key, count, TO_INTEGER);
	}

	public <T> List<T> sRandomFor(String key, long count, Class<T> clazz) {
		return sRandomFor(key, count, this.toObject(clazz));
	}

	public <T> List<T> sRandomFor(String key, long count, TypeReference<T> typeRef) {
		return sRandomFor(key, count, this.toObject(typeRef));
	}

	public <T> List<T> sRandomFor(String key, long count, Function<Object, T> mapper) {
		List<Object> members = this.sRandom(key, count);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toList());
		}
		return null;
	}

	public List<Object> sRandom(String key, long count) {
		try {
			return getSetOperations().randomMembers(key, count);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 随机获取指定数量的元素,去重(同一个元素只能选择一次)
	 *
	 * @param key 缓存key
	 * @param count 随机获取元素数量
	 * @return 随机获取的元素集合（不重复）
	 */
	public Set<String> sRandomDistinctString(String key, long count) {
		return sRandomDistinctFor(key, count, TO_STRING);
	}

	public Set<Double> sRandomDistinctDouble(String key, long count) {
		return sRandomDistinctFor(key, count, TO_DOUBLE);
	}

	public Set<Long> sRandomDistinctLong(String key, long count) {
		return sRandomDistinctFor(key, count, TO_LONG);
	}

	public Set<Integer> sRandomDistinctInteger(String key, long count) {
		return sRandomDistinctFor(key, count, TO_INTEGER);
	}

	public <T> Set<T> sRandomDistinctFor(String key, long count, Class<T> clazz) {
		return sRandomDistinctFor(key, count, this.toObject(clazz));
	}

	public <T> Set<T> sRandomDistinctFor(String key, long count, TypeReference<T> typeRef) {
		return sRandomDistinctFor(key, count, this.toObject(typeRef));
	}

	public <T> Set<T> sRandomDistinctFor(String key, long count, Function<Object, T> mapper) {
		Set<Object> members = this.sRandomDistinct(key, count);
		if (Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	public Set<Object> sRandomDistinct(String key, long count) {
		try {
			return getSetOperations().distinctRandomMembers(key, count);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除值为value的
	 *
	 * @param key    键
	 * @param values 值 可以是多个
	 * @return 移除的个数
	 */
	public Long sRemove(String key, Object... values) {
		try {
			Long count = getSetOperations().remove(key, values);
			return count;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void sScan(String bigSetKey, Consumer<byte[]> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).build();
		this.sScan(bigSetKey, options, consumer);
	}

	public void sScan(String bigSetKey, String pattern, Consumer<byte[]> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build();
		this.sScan(bigSetKey, options, consumer);
	}

	public void sScan(String bigSetKey, ScanOptions options, Consumer<byte[]> consumer) {
		this.redisTemplate.execute((RedisConnection redisConnection) -> {
			try (Cursor<byte[]> cursor = redisConnection.sScan(rawKey(bigSetKey), options)) {
				cursor.forEachRemaining(consumer);
				return null;
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RedisOperationException(e.getMessage());
			}
		});
	}

	/**
	 * 将set数据放入缓存
	 *
	 * @param key     键
	 * @param seconds 过期时长(秒)
	 * @param values  值 可以是多个
	 * @return 成功个数
	 */
	public Long sSetAndTime(String key, long seconds, Object... values) {
		try {
			Long count = getSetOperations().add(key, values);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return count;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取set缓存的长度
	 *
	 * @param key 缓存key
	 * @return set缓存的长度
	 */
	public Long sSize(String key) {
		try {
			return getSetOperations().size(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Set<Object> sUnion(String key, String otherKey) {
		try {
			return getSetOperations().union(key, otherKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Set<Object> sUnion(String key, Collection<String> keys) {
		try {
			return getSetOperations().union(key, keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 合并所有指定keys的数据
	 *
	 * @param keys 键集合
	 * @return 返回成功数据
	 */
	public Set<Object> sUnion(Collection<String> keys) {
		try {
			return getSetOperations().union(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sUnionAndStore(String key, String otherKey, String destKey) {
		try {
			return getSetOperations().unionAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long sUnionAndStore(String key, Collection<String> keys, String destKey) {
		try {
			return getSetOperations().unionAndStore(key, keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 合并所有指定keys的数据，存储到destKey中
	 *
	 * @param keys    键集合
	 * @param destKey 键
	 * @return 返回成功数据
	 */
	public Long sUnionAndStore(Collection<String> keys, String destKey) {
		try {
			return getSetOperations().unionAndStore(keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	protected SetOperations<String, Object> getSetOperations() {
		return redisTemplate.opsForSet();
	}

	// ===============================ZSet=================================
	// 相关命令：https://www.redis.net.cn/order/3627.html
	// ====================================================================

	/**
	 * 向有序集合添加一个成员，或者更新已存在成员的分数
	 * https://www.redis.net.cn/order/3609.html
	 * @param key 缓存key
	 * @param value	成员元素
	 * @param score 成员的分数值
	 * @return 是否添加成功
	 */
	public Boolean zAdd(String key, Object value, double score) {
		try {
			return getZSetOperations().add(key, value, score);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向有序集合添加一个成员，或者更新已存在成员的分数
	 * https://www.redis.net.cn/order/3609.html
	 * @param key 缓存key
	 * @param value	成员元素
	 * @param score 成员的分数值
	 * @param seconds 过期时长（秒）
	 * @return 是否添加成功
	 */
	public Boolean zAdd(String key, Object value, double score, long seconds) {
		try {
			Boolean result = getZSetOperations().add(key, value, score);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向有序集合添加一个成员，或者更新已存在成员的分数
	 * https://www.redis.net.cn/order/3609.html
	 * @param key 缓存key
	 * @param value	成员元素
	 * @param score 成员的分数值
	 * @param timeout 过期时长
	 * @return 是否添加成功
	 */
	public Boolean zAdd(String key, Object value, double score, Duration timeout) {
		try {
			Boolean result = getZSetOperations().add(key, value, score);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向有序集合添加一个成员，或者更新已存在成员的分数
	 * https://www.redis.net.cn/order/3609.html
	 * @param key 缓存key
	 * @param tuples 成员元素
	 * @return 是否添加成功
	 */
	public Long zAdd(String key, Set<TypedTuple<Object>> tuples) {
		try {
			return getZSetOperations().add(key, tuples);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向有序集合添加多个成员，或者更新已存在成员的分数
	 * https://www.redis.net.cn/order/3609.html
	 * @param key 缓存key
	 * @param tuples 成员元素
	 * @param seconds 过期时长（秒）
	 * @return 是否添加成功
	 */
	public Long zAdd(String key, Set<TypedTuple<Object>> tuples, long seconds) {
		try {
			Long result = getZSetOperations().add(key, tuples);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 向有序集合添加多个成员，或者更新已存在成员的分数
	 * https://www.redis.net.cn/order/3609.html
	 * @param key 缓存key
	 * @param tuples 成员元素
	 * @param timeout 过期时长
	 * @return 是否添加成功
	 */
	public Long zAdd(String key, Set<TypedTuple<Object>> tuples, Duration timeout) {
		try {
			Long result = getZSetOperations().add(key, tuples);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取有序集合的成员数
	 * https://www.redis.net.cn/order/3610.html
	 * @param key 缓存key
	 * @return 有序集合的成员数
	 */
	public Long zCard(String key) {
		try {
			return getZSetOperations().zCard(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集中，指定成员是否存在
	 * https://www.redis.net.cn/order/3626.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @return 是否存在
	 */
	public Boolean zHas(String key, Object value) {
		try {
			return getZSetOperations().score(key, value) != null;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算在有序集合中指定区间分数的成员数
	 * https://www.redis.net.cn/order/3611.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 在有序集合中指定区间分数的成员数
	 */
	public Long zCount(String key, double min, double max) {
		try {
			return getZSetOperations().count(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * Set删除: sscan + srem
	 *
	 * @param bigZsetKey 键
	 * @return 是否删除成功
	 */
	public Boolean zDel(String bigZsetKey) {
		try {
			this.zScan(bigZsetKey, (tuple) -> {
				this.zRem(bigZsetKey, deserializeTuple(tuple).getValue());
			});
			return redisTemplate.delete(bigZsetKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 有序集合中对指定成员的分数加上增量 increment
	 * https://www.redis.net.cn/order/3612.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @param delta 增量值
	 * @return 增加后的分数值
	 */
	public Double zIncr(String key, Object value, double delta) {
		try {
			return getZSetOperations().incrementScore(key, value, delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 有序集合中对指定成员的分数加上增量 increment，并给key设置过期时间
	 * https://www.redis.net.cn/order/3612.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @param delta 增量值
	 * @param seconds 过期时长（秒）
	 * @return 增加后的分数值
	 */
	public Double zIncr(String key, Object value, double delta, long seconds) {
		try {
			Double result = getZSetOperations().incrementScore(key, value, delta);
			if (seconds > 0) {
				expire(key, seconds);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 有序集合中对指定成员的分数加上增量 increment，并给key设置过期时间
	 * https://www.redis.net.cn/order/3612.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @param delta 增量值
	 * @param timeout 过期时长
	 * @return 增加后的分数值
	 */
	public Double zIncr(String key, Object value, double delta, Duration timeout) {
		try {
			Double result = getZSetOperations().incrementScore(key, value, delta);
			if (!timeout.isNegative()) {
				expire(key, timeout);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * zset指定元素增加值，并监听指定区域的顺序变化，如果指定区域元素发送变化，则返回true
	 * @param key 缓存key
	 * @param value 成员元素
	 * @param delta 增量值
	 * @param start 开始位置
	 * @param end 结束位置
	 * @return  指定区域元素发生变化返回true，否则返回false
	 */
	public Boolean zIncrAndWatch(String key, Object value, double delta, long start, long end) {
		try {
			byte[] rawKey = rawKey(key);
			byte[] rawValue = rawValue(value);
			return this.redisTemplate.execute((RedisConnection redisConnection) -> {
				// 1、增加score之前查询指定区域的元素对象
				Set<TypedTuple<Object>> zset1 = deserializeTupleValues(redisConnection.zRevRangeWithScores(rawKey, start, end));
				// 2、增加score
				redisConnection.zIncrBy(rawKey, delta, rawValue);
				// 3、增加score之后查询指定区域的元素对象
				Set<TypedTuple<Object>> zset2 = deserializeTupleValues(redisConnection.zRevRangeWithScores(rawKey, start, end));
				// 4、如果同一key两次取值有一个为空，表示元素发生了新增或移除，那两个元素一定有变化了
				if(CollectionUtils.isEmpty(zset1) && !CollectionUtils.isEmpty(zset2) || !CollectionUtils.isEmpty(zset1) && CollectionUtils.isEmpty(zset2)) {
					return Boolean.TRUE;
				}
				// 5、如果两个元素都不为空，但是长度不相同，表示元素一定有变化了
				if(zset1.size() != zset2.size()) {
					return Boolean.TRUE;
				}
				// 6、 两个set都不为空，且长度相同，则对key进行提取，并比较keyList与keyList2,一旦遇到相同位置处的值不一样，表示顺序发生了变化
				List<String> keyList1 = Objects.isNull(zset1) ? Lists.newArrayList() : zset1.stream().map(item -> item.getValue().toString()).collect(Collectors.toList());
				List<String> keyList2 = Objects.isNull(zset2) ? Lists.newArrayList() : zset2.stream().map(item -> item.getValue().toString()).collect(Collectors.toList());
				for (int i = 0; i < keyList1.size(); i++) {
					if(!Objects.equals(keyList1.get(i), keyList2.get(i))) {
						return Boolean.TRUE;
					}
				}
				return Boolean.FALSE;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的交集并将结果集存储在新的有序集合 key 中
	 * https://www.redis.net.cn/order/3613.html
	 * @param key 第一个有序集合的键
	 * @param otherKey 第二个有序集合的键
	 * @param destKey 目标键
	 * @return 保存到 destKey 的结果集的基数
	 */
	public Long zIntersectAndStore(String key, String otherKey, String destKey) {
		try {
			return getZSetOperations().intersectAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的交集并将结果集存储在新的有序集合 key 中
	 * https://www.redis.net.cn/order/3613.html
	 * @param key 第一个有序集合的键
	 * @param otherKeys 其他多个有序集合Key的集合
	 * @param destKey 目标键
	 * @return 保存到 destKey 的结果集的基数
	 */
	public Long zIntersectAndStore(String key, Collection<String> otherKeys, String destKey) {
		try {
			return getZSetOperations().intersectAndStore(key, otherKeys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的交集并将结果集存储在新的有序集合 key 中
	 * https://www.redis.net.cn/order/3613.html
	 * @param key 第一个有序集合的键
	 * @param otherKeys 其他多个有序集合Key的集合
	 * @param destKey 目标键
	 * @param aggregate 聚合方式（SUM, MIN, MAX）
	 * @return 保存到 destKey 的结果集的基数
	 */
	public Long zIntersectAndStore(String key, Collection<String> otherKeys, String destKey, Aggregate aggregate) {
		try {
			return getZSetOperations().intersectAndStore(key, otherKeys, destKey, aggregate);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的交集并将结果集存储在新的有序集合 key 中
	 * https://www.redis.net.cn/order/3613.html
	 * @param key 第一个有序集合的键
	 * @param otherKeys 其他多个有序集合Key的集合
	 * @param destKey 目标键
	 * @param aggregate 聚合方式（SUM, MIN, MAX）
	 * @param weights 权重
	 * @return 保存到 destKey 的结果集的基数
	 */
	public Long zIntersectAndStore(String key, Collection<String> otherKeys, String destKey, Aggregate aggregate, Weights weights) {
		try {
			return getZSetOperations().intersectAndStore(key, otherKeys, destKey, aggregate, weights);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除有序集合中的一个或多个成员
	 * https://www.redis.net.cn/order/3619.html
	 * @param key 缓存key
	 * @param values 要移除的value数组
	 * @return 移除的元素个数
	 */
	public Long zRem(String key, Object... values) {
		try {
			return getZSetOperations().remove(key, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除有序集中，指定分数（score）区间内的所有成员
	 * https://www.redis.net.cn/order/3622.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 移除的元素个数
	 */
	public Long zRemByScore(String key, double min, double max) {
		try {
			return getZSetOperations().removeRangeByScore(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员，并转换成String类型集合
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<String> zRangeString(String key, long start, long end) {
		return zRangeFor(key, start, end, TO_STRING);
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员，并转换成Double类型集合
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<Double> zRangeDouble(String key, long  start, long end) {
		return zRangeFor(key, start, end, TO_DOUBLE);
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员，并转换成Long类型集合
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<Long> zRangeLong(String key, long  start, long end) {
		return zRangeFor(key, start, end, TO_LONG);
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员，并转换成Integer类型集合
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<Integer> zRangeInteger(String key, long  start, long end) {
		return zRangeFor(key, start, end, TO_INTEGER);
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员，并转换成指定类型对象集合
	 * https://www.redis.net.cn/order/3615.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @param clazz 期望返回的类型 class
	 * @return 指定区间内的成员
	 * @param <T> 期望返回的类型
	 */
	public <T> Set<T> zRangeFor(String key, long start, long end, Class<T> clazz) {
		return zRangeFor(key, start, end, this.toObject(clazz));
	}

	public <T> Set<T> zRangeFor(String key, long start, long end, TypeReference<T> typeRef) {
		return zRangeFor(key, start, end, this.toObject(typeRef));
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员
	 * https://www.redis.net.cn/order/3615.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @param mapper 类型转换函数
	 * @return 指定区间内的成员
	 */
	public <T> Set<T> zRangeFor(String key, long  start, long end, Function<Object, T> mapper) {
		Set<Object> members = this.zRange(key, start, end);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper)
					.collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员
	 * https://www.redis.net.cn/order/3615.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<Object> zRange(String key, long start, long end) {
		try {
			return getZSetOperations().range(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 移除有序集中，指定分数（score）区间内的所有成员
	 * https://www.redis.net.cn/order/3622.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 移除的元素个数
	 */
	public Set<String> zRangeStringByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_STRING);
	}

	public Set<Double> zRangeDoubleByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_DOUBLE);
	}

	public Set<Long> zRangeLongByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_LONG);
	}

	public Set<Integer> zRangeIntegerByScore(String key, double min, double max) {
		return zRangeByScoreFor(key, min, max, TO_INTEGER);
	}

	public <T> Set<T> zRangeByScoreFor(String key, double min, double max, Class<T> clazz) {
		return zRangeByScoreFor(key, min, max, this.toObject(clazz));
	}

	public <T> Set<T> zRangeByScoreFor(String key, double min, double max, TypeReference<T> typeRef) {
		return zRangeByScoreFor(key, min, max, this.toObject(typeRef));
	}

	/**
	 * 移除有序集中，指定分数（score）区间内的所有成员
	 * https://www.redis.net.cn/order/3622.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 移除的元素个数
	 */
	public <T> Set<T> zRangeByScoreFor(String key, double min, double max, Function<Object, T> mapper) {
		Set<Object> members = this.zRangeByScore(key, min, max);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper)
					.collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 返回有序集合中指定分数区间的成员列表。有序集成员按分数值递增(从小到大)次序排列。
	 * https://www.redis.net.cn/order/3617.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定分数区间的成员列表
	 */
	public Set<Object> zRangeByScore(String key, double min, double max) {
		try {
			return getZSetOperations().rangeByScore(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 通过索引区间返回有序集合成指定区间内的成员。其中成员的位置按分数值递增(从小到大)来排序。
	 * https://www.redis.net.cn/order/3615.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<TypedTuple<Object>> zRangeWithScores(String key, long start, long end) {
		try {
			return getZSetOperations().rangeWithScores(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集合中指定分数区间的成员列表。有序集成员按分数值递增(从小到大)次序排列。
	 * https://www.redis.net.cn/order/3617.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定分数区间的成员列表
	 */
	public Set<TypedTuple<Object>> zRangeByScoreWithScores(String key, double min, double max) {
		try {
			return getZSetOperations().rangeByScoreWithScores(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 通过字典区间返回有序集合的成员
	 * https://www.redis.net.cn/order/3616.html
	 * @param key 缓存key
	 * @param range 字典区间
	 * @return 指定字典区间的成员列表
	 */
	public Set<Object> zRangeByLex(String key, Range range) {
		try {
			return getZSetOperations().rangeByLex(key, range);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 通过字典区间返回有序集合的成员
	 * https://www.redis.net.cn/order/3616.html
	 * @param key 缓存key
	 * @param range 字典区间
	 * @param limit 限制
	 * @return 指定字典区间的成员列表
	 */
	public Set<Object> zRangeByLex(String key, Range range, Limit limit) {
		try {
			return getZSetOperations().rangeByLex(key, range, limit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内转换成String类型的成员
	 */
	public Set<String> zRevrangeString(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_STRING);
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内转换成Double类型的成员
	 */
	public Set<Double> zRevrangeDouble(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_DOUBLE);
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内转换成Long类型的成员
	 */
	public Set<Long> zRevrangeLong(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_LONG);
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内转换成Integer类型的成员
	 */
	public Set<Integer> zRevrangeInteger(String key, long  start, long end) {
		return zRevrangeFor(key, start, end, TO_INTEGER);
	}


	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @param clazz 目标对象类型
	 * @return 指定区间内转换成目标对象类型的成员
	 */
	public <T> Set<T> zRevrangeFor(String key, long start, long end, Class<T> clazz) {
		return zRevrangeFor(key, start, end, this.toObject(clazz));
	}

	public <T> Set<T> zRevrangeFor(String key, long start, long end, TypeReference<T> typeRef) {
		return zRevrangeFor(key, start, end, this.toObject(typeRef));
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @param mapper 类型转换函数
	 * @return 指定区间内转换函数转换后的的成员
	 */
	public <T> Set<T> zRevrangeFor(String key, long  start, long end, Function<Object, T> mapper) {
		Set<Object> members = this.zRevrange(key, start, end);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<Object> zRevrange(String key, long start, long end) {
		try {
			return getZSetOperations().reverseRange(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集中，指定区间内的成员。其中成员的位置按分数值递减(从大到小)来排列。
	 * https://www.redis.net.cn/order/3623.html
	 * @param key 缓存key
	 * @param start 开始索引
	 * @param end 结束索引
	 * @return 指定区间内的成员
	 */
	public Set<TypedTuple<Object>> zRevrangeWithScores(String key, long start, long end) {
		try {
			return getZSetOperations().reverseRangeWithScores(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定区间内转换成String类型的成员
	 */
	public Set<String> zRevrangeStringByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_STRING);
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定区间内转换成Double类型的成员
	 */
	public Set<Double> zRevrangeDoubleByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_DOUBLE);
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定区间内转换成Long类型的成员
	 */
	public Set<Long> zRevrangeLongByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_LONG);
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定区间内转换成Integer类型的成员
	 */
	public Set<Integer> zRevrangeIntegerByScore(String key, double min, double max) {
		return zRevrangeForByScore(key, min, max, TO_INTEGER);
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @param clazz 目标对象类型
	 * @return 指定区间内转换成目标对象类型的成员
	 */
	public <T> Set<T> zRevrangeForByScore(String key, double min, double max, Class<T> clazz) {
		return zRevrangeForByScore(key, min, max, this.toObject(clazz));
	}

	public <T> Set<T> zRevrangeForByScore(String key, double min, double max, TypeReference<T> typeRef) {
		return zRevrangeForByScore(key, min, max, this.toObject(typeRef));
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @param mapper 类型转换函数
	 * @return 指定区间内转换函数转换后的的成员
	 */
	public <T> Set<T> zRevrangeForByScore(String key, double min, double max, Function<Object, T> mapper) {
		Set<Object> members = this.zRevrangeByScore(key, min, max);
		if(Objects.nonNull(members)) {
			return members.stream().map(mapper).collect(Collectors.toCollection(LinkedHashSet::new));
		}
		return null;
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定分数区间的成员列表
	 */
	public Set<Object> zRevrangeByScore(String key, double min, double max) {
		try {
			return getZSetOperations().reverseRangeByScore(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
	 * https://www.redis.net.cn/order/3624.html
	 * @param key 缓存key
	 * @param min 最小分数
	 * @param max 最大分数
	 * @return 指定分数区间的成员列表
	 */
	public Set<TypedTuple<Object>> zRevrangeByScoreWithScores(String key, double min, double max) {
		try {
			return getZSetOperations().reverseRangeByScoreWithScores(key, min, max);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 返回有序集中成员的排名。其中有序集成员按分数值递减(从大到小)排序。
	 * 排名以 0 为底，也就是说， 分数值最大的成员排名为 0 。
	 * https://www.redis.net.cn/order/3625.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @return 指定成员的排名
	 */
	public Long zRevRank(String key, Object value) {
		try {
			return getZSetOperations().reverseRank(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 迭代有序集合中的元素（包括元素成员和元素分值）
	 * https://www.redis.net.cn/order/3628.html
	 * @param bigZsetKey 缓存key
	 * @param consumer 消费者
	 */
	public void zScan(String bigZsetKey, Consumer<Tuple> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).build();
		this.zScan(bigZsetKey, options, consumer);
	}

	/**
	 * 迭代有序集合中的元素（包括元素成员和元素分值）
	 * https://www.redis.net.cn/order/3628.html
	 * @param bigZsetKey 缓存key
	 * @param pattern 匹配表达式
	 * @param consumer 消费者
	 */
	public void zScan(String bigZsetKey, String pattern, Consumer<Tuple> consumer) {
		ScanOptions options = ScanOptions.scanOptions().count(Long.MAX_VALUE).match(pattern).build();
		this.zScan(bigZsetKey, options, consumer);
	}

	/**
	 * 迭代有序集合中的元素（包括元素成员和元素分值）
	 * https://www.redis.net.cn/order/3628.html
	 * @param bigZsetKey 缓存key
	 * @param options 扫描选项
	 * @param consumer 消费者
	 */
	public void zScan(String bigZsetKey, ScanOptions options, Consumer<Tuple> consumer) {
		this.redisTemplate.execute((RedisConnection redisConnection) -> {
			try (Cursor<Tuple> cursor = redisConnection.zScan(rawKey(bigZsetKey), options)) {
				cursor.forEachRemaining(consumer);
				return null;
			} catch (Exception e) {
				log.error(e.getMessage());
				throw new RedisOperationException(e.getMessage());
			}
		});
	}

	/**
	 * 返回有序集中，指定成员的分数值
	 * https://www.redis.net.cn/order/3626.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @param defaultVal 默认值
	 * @return 成员的分数值
	 */
	public Double zScore(String key, Object value, double defaultVal) {
		Double rtVal = zScore(key, value);
		return Objects.nonNull(rtVal) ? rtVal : defaultVal;
	}

	/**
	 * 返回有序集中，指定成员的分数值
	 * https://www.redis.net.cn/order/3626.html
	 * @param key 缓存key
	 * @param value 成员元素
	 * @return 成员的分数值
	 */
	public Double zScore(String key, Object value) {
		try {
			return getZSetOperations().score(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的并集，并存储在新的缓存Key 中
	 * https://www.redis.net.cn/order/3627.html
	 * @param key 缓存key
	 * @param otherKey 另外一个缓存Key
	 * @param destKey 目标缓存Key
	 * @return 结果集中的成员数量
	 */
	public Long zUnionAndStore(String key, String otherKey, String destKey) {
		try {
			return getZSetOperations().unionAndStore(key, otherKey, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的并集，并存储在新的缓存Key 中
	 * https://www.redis.net.cn/order/3627.html
	 * @param key 缓存key
	 * @param keys 其他缓存Key集合
	 * @param destKey 目标缓存Key
	 * @return 结果集中的成员数量
	 */
	public Long zUnionAndStore(String key, Collection<String> keys, String destKey) {
		try {
			return getZSetOperations().unionAndStore(key, keys, destKey);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的并集，并存储在新的缓存Key 中
	 * https://www.redis.net.cn/order/3627.html
	 * @param key 缓存key
	 * @param keys 其他缓存Key集合
	 * @param destKey 目标缓存Key
	 * @param aggregate 聚合方式（SUM, MIN, MAX）
	 * @return 结果集中的成员数量
	 */
	public Long zUnionAndStore(String key, Collection<String> keys, String destKey, Aggregate aggregate) {
		try {
			return getZSetOperations().unionAndStore(key, keys, destKey, aggregate);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 计算给定的一个或多个有序集的并集，并存储在新的缓存Key 中
	 * https://www.redis.net.cn/order/3627.html
	 * @param key 缓存key
	 * @param keys 其他缓存Key集合
	 * @param destKey 目标缓存Key
	 * @param aggregate 聚合方式（SUM, MIN, MAX）
 	 * @param weights 权重
	 * @return 结果集中的成员数量
	 */
	public Long zUnionAndStore(String key, Collection<String> keys, String destKey, Aggregate aggregate, Weights weights) {
		try {
			return getZSetOperations().unionAndStore(key, keys, destKey, aggregate, weights);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	protected ZSetOperations<String, Object> getZSetOperations() {
		return redisTemplate.opsForZSet();
	}

	// ===============================Stream=================================

	public RecordId xAdd(String key, Map<String,Object> message){
		try {
			RecordId add = getStreamOperations().add(key, message);
			return add;	//返回增加后的id
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long xTrim(String key, long count){
		try {
			Long ct = getStreamOperations().trim(key, count);
			return ct;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long xDel(String key, String... recordIds){
		try {
			Long ct = getStreamOperations().delete(key, recordIds);
			return ct;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long xDel(String key, RecordId... recordIds){
		try {
			Long ct = getStreamOperations().delete(key, recordIds);
			return ct;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long xLen(String key){
		try {
			Long ct = getStreamOperations().size(key);
			return ct;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<MapRecord<String, Object, Object>> xRange(String key, org.springframework.data.domain.Range<String> range){
		try {
			return getStreamOperations().range(key, range);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<MapRecord<String, Object, Object>> xRange(String key, org.springframework.data.domain.Range<String> range, Limit limit){
		try {
			return getStreamOperations().range(key, range, limit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> List<ObjectRecord<String, V>> xRangeFor(Class<V> targetType, String key, org.springframework.data.domain.Range<String> range){
		try {
			return getStreamOperations().range(targetType, key, range);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public <V> List<ObjectRecord<String, V>> xRangeFor(Class<V> targetType, String key, org.springframework.data.domain.Range<String> range, Limit limit){
		try {
			return getStreamOperations().range(targetType, key, range, limit);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public String addGroup(String key, String groupName){
		try {
			return getStreamOperations().createGroup(key, groupName);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	protected StreamOperations<String, Object, Object> getStreamOperations() {
		return redisTemplate.opsForStream();
	}

	// ===============================HyperLogLog=================================

	public Long pfAdd(String key, Object... values) {
		try {
			return getHyperLogLogOperations().add(key, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Boolean pfDel(String key) {
		try {
			getHyperLogLogOperations().delete(key);
			return Boolean.TRUE;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long pfCount(String... keys) {
		try {
			return getHyperLogLogOperations().size(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long pfMerge(String destination, String... sourceKeys) {
		try {
			return getHyperLogLogOperations().union(destination, sourceKeys);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	protected HyperLogLogOperations<String, Object> getHyperLogLogOperations() {
		return redisTemplate.opsForHyperLogLog();
	}


	// ===============================BitMap=================================

	/**
	 * 设置ASCII码, 字符串'a'的ASCII码是97, 转为二进制是'01100001', 此方法是将二进制第offset位值变为value
	 *
	 * @param key 缓存key
	 * @param offset 偏移量
	 * @param value 值,true为1, false为0
	 * @return 是否设置成功
	 */
	public Boolean setBit(String key, long offset, boolean value) {
		try {
			return getValueOperations().setBit(key, offset, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)
	 *
	 * @param key 缓存key
	 * @param offset 偏移量
	 * @return 是否有值
	 */
	public Boolean getBit(String key, long offset) {
		try {
			return getValueOperations().getBit(key, offset);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 追加到末尾
	 *
	 * @param key  缓存key
	 * @param value 字符串
	 * @return 追加结果
	 */
	public Integer append(String key, String value) {
		try {
			return getValueOperations().append(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	protected ValueOperations<String, Object> getValueOperations() {
		return redisTemplate.opsForValue();
	}

	// ===============================Message=================================

	/**
	 * 发送消息
	 *
	 * @param channel 消息channel
	 * @param message 消息内容
	 */
	public void sendMessage(String channel, String message) {
		try {
			redisTemplate.convertAndSend(channel, message);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================Lock=================================

	/**
	 * 1、对指定key来进行加锁逻辑（此锁是分布式阻塞锁）
	 * https://www.jianshu.com/p/6dbc44defd94
	 * @param lockKey  锁 key
	 * @param seconds  最大阻塞时间(秒)，超过时间将不再等待拿锁
	 * @return 获取锁成功/失败
	 */
	public boolean tryBlockLock(String lockKey, int seconds) {
        try {
			return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
			    // 1、获取时间毫秒值
			    long expireAt = redisConnection.time() + seconds * 1000 + 1;
			    // 2、第一次请求, 锁标识不存在的情况，直接拿到锁
			    Boolean acquire = redisConnection.setNX(rawKey(lockKey), String.valueOf(expireAt).getBytes());
			    if (acquire) {
			        return true;
			    } else {
			    	// 3、非第一次请求，阻塞等待拿到锁
			    	redisConnection.bRPop(seconds, rawKey(lockKey + ":list"));
			    }
			    return false;
			});
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
		}
       	return false;
    }

	/**
	 * 2、删除指定key来进行完成解锁逻辑
	 * @param lockKey  锁key
	 * @param requestId  锁值
	 * @return 释放锁成功/失败
	 */
    public boolean unBlockLock(String lockKey, String requestId) {
    	try {
    		return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
    			redisConnection.del(rawKey(lockKey));
    			byte[] rawKey = rawKey(lockKey + ":list");
    			byte[] rawValue = rawValue(requestId);
    			redisConnection.rPush(rawKey, rawValue);
    		    return true;
    		}, true);
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
			throw new RedisOperationException(e.getMessage());
		}
	}

	public boolean tryLock(String lockKey, Duration timeout) {
		return tryLock( lockKey, timeout.toMillis());
	}

	/**
	 * 1、对指定key来进行加锁逻辑（此锁是全局性的）
	 * @param lockKey  锁key
	 * @param expireMillis 锁有效期
	 * @return 是否加锁成功
	 */
	public boolean tryLock(String lockKey, long expireMillis) {
        try {
			return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
				byte[] serLockKey = rawString(lockKey);
			    // 1、获取时间毫秒值
			    long expireAt = redisConnection.time() + expireMillis + 1;
			    // 2、获取锁
			    Boolean acquire = redisConnection.setNX(serLockKey, String.valueOf(expireAt).getBytes());
			    if (acquire) {
			        return true;
			    } else {
			        byte[] bytes = redisConnection.get(serLockKey);
			        // 3、非空判断
			        if (Objects.nonNull(bytes) && bytes.length > 0) {
			            long expireTime = Long.parseLong(new String(bytes));
			            // 4、如果锁已经过期
			            if (expireTime < redisConnection.time()) {
			                // 5、重新加锁，防止死锁
			                byte[] set = redisConnection.getSet(serLockKey, String.valueOf(redisConnection.time() + expireMillis + 1).getBytes());
			                return Long.parseLong(new String(set)) < redisConnection.time();
			            }
			        }
			    }
			    return false;
			});
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
		}
       	return false;
    }

	/**
	 * 2、删除指定key来进行完成解锁逻辑
	 * @param lockKey  锁key
	 * @return 是否解锁成功
	 */
    public boolean unlock(String lockKey) {
    	try {
	        return redisTemplate.delete(lockKey);
        } catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
			throw new RedisOperationException(e.getMessage());
		}
	}

    public boolean tryLock(String lockKey, String requestId, Duration timeout, int retryTimes, long retryInterval) {
    	return tryLock(lockKey, requestId, timeout.toMillis(), retryTimes, retryInterval);
    }

    /**
	 * 1、lua脚本加锁
	 * @param lockKey       锁的 key
	 * @param requestId     锁的 value
	 * @param expire        key 的过期时间，单位 ms
	 * @param retryTimes    重试次数，即加锁失败之后的重试次数
	 * @param retryInterval 重试时间间隔，单位 ms
	 * @return 加锁 true 成功
	 */
	public boolean tryLock(String lockKey, String requestId, long expire, int retryTimes, long retryInterval) {
       try {
			return redisTemplate.execute((RedisCallback<Boolean>) redisConnection -> {
				// 1、执行lua脚本
				Long result =  this.executeLuaScript(LOCK_LUA_SCRIPT, Collections.singletonList(lockKey), requestId, expire);
				if(LOCK_SUCCESS.equals(result)) {
				    log.info("locked... redisK = {}", lockKey);
				    return true;
				} else {
					// 2、重试获取锁
			        int count = 0;
			        while(count < retryTimes) {
			            try {
			                Thread.sleep(retryInterval);
			                result = this.executeLuaScript(LOCK_LUA_SCRIPT, Collections.singletonList(lockKey), requestId, expire);
			                if(LOCK_SUCCESS.equals(result)) {
			                	log.info("locked... redisK = {}", lockKey);
			                    return true;
			                }
			                log.warn("{} times try to acquire lock", count + 1);
			                count++;
			            } catch (Exception e) {
			            	log.error("acquire redis occurred an exception", e);
			            }
			        }
			        log.info("fail to acquire lock {}", lockKey);
			        return false;
				}
			});
		} catch (Exception e) {
			log.error("acquire redis occurred an exception", e);
		}
       	return false;
	}

	/**
	 * 2、lua脚本释放KEY
	 * @param lockKey 释放本请求对应的锁的key
	 * @param requestId   释放本请求对应的锁的value
	 * @return 释放锁 true 成功
	 */
    public boolean unlock(String lockKey, String requestId) {
        log.info("unlock... redisK = {}", lockKey);
        try {
            // 使用lua脚本删除redis中匹配value的key
            Long result = this.executeLuaScript(UNLOCK_LUA_SCRIPT, Collections.singletonList(lockKey), requestId);
            //如果这里抛异常，后续锁无法释放
            if (LOCK_SUCCESS.equals(result)) {
            	log.info("release lock success. redisK = {}", lockKey);
                return true;
            } else if (LOCK_EXPIRED.equals(result)) {
            	log.warn("release lock exception, key has expired or released");
            } else {
                //其他情况，一般是删除KEY失败，返回0
            	log.error("release lock failed");
            }
        } catch (Exception e) {
        	log.error("release lock occurred an exception", e);
			throw new RedisOperationException(e.getMessage());
        }
        return false;
    }

	// ===============================Pipeline=================================

	public List<Object> executePipelined(RedisCallback<?> action) {
		try {
			return redisTemplate.executePipelined(action);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public List<Object> executePipelined(RedisCallback<?> action, RedisSerializer<?> resultSerializer) {
		try {
			return redisTemplate.executePipelined(action, resultSerializer);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================RedisScript=================================

	/**
     * 库存增加
     * @param key   库存key
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Long luaIncr(String key, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(INCR_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存增加
     * @param key   库存key
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Double luaIncr(String key, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(INCR_BYFLOAT_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Long luaDecr(String key, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(DECR_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 库存除以指定数值
	 * @param key   库存key
	 * @param delta 被除数
	 * @return
	 *      -3:代表传进来的被除数值是非正数（非法值）
	 *      -2:库存未初始化
	 *      -1:库存不足
	 *      大于等于0: 除法运算后的结果
	 */
	public Long luaDiv(String key, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(DIV_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Double luaDecr(String key, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return this.executeLuaScript(DECR_BYFLOAT_SCRIPT, Lists.newArrayList(key), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}

	}

	/**
     * 库存增加
     * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Long luaHincr(String key, String hashKey, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return redisTemplate.execute(HINCR_SCRIPT, this.hashValueSerializer(), this.hashValueSerializer(),
					Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存增加
     * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 增加数量
     * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * 大于等于0:剩余库存（新增之后剩余的库存）
     */
	public Double luaHincr(String key, String hashKey, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return redisTemplate.execute(HINCR_BYFLOAT_SCRIPT, this.hashValueSerializer(),
					this.hashValueSerializer(), Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Long luaHdecr(String key, String hashKey, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return redisTemplate.execute(HDECR_SCRIPT, this.hashValueSerializer(), this.hashValueSerializer(),
					Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
     * 库存扣减
	 * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 扣减数量
	 * @return
     * -4:代表库存传进来的值是负数（非法值）
     * -3:库存未初始化
     * -2:库存不足
     * -1:库存为0
     * 大于等于0:剩余库存（扣减之后剩余的库存）
	 */
	public Double luaHdecr(String key, String hashKey, double delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return redisTemplate.execute(HDECR_BYFLOAT_SCRIPT, this.hashValueSerializer(),
					this.hashValueSerializer(), Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 库存除以指定值
	 * @param key   库存key
	 * @param hashKey Hash键
	 * @param delta 被除数
	 * @return
	 *      -3:代表传进来的被除数值是非正数（非法值）
	 *      -2:库存未初始化
	 *      -1:库存不足
	 *      大于等于0: 除法运算后的结果
	 */
	public Long luaHdiv(String key, String hashKey, long delta) {
		Assert.hasLength(key, "key must not be empty");
		try {
			return redisTemplate.execute(HDIV_SCRIPT, this.hashValueSerializer(),
					this.hashValueSerializer(), Lists.newArrayList(key, hashKey), delta);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行lua脚本
	 *
	 * @param luaScript  脚本内容
	 * @param returnType 返回值类型
	 * @param keys       redis键列表
	 * @param values     值列表
	 * @param <R> 返回类型
	 * @return lua脚步执行结果
	 */
	public <R> R executeLuaScript(String luaScript, Class<R> returnType, List<String> keys, Object... values) {
		try {
			RedisScript redisScript = RedisScript.of(luaScript, returnType);
			return (R) redisTemplate.execute(redisScript, keys, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行lua脚本
	 *
	 * @param luaScript 脚本内容
	 * @param keys      redis键列表
	 * @param values    值列表
	 * @param <R> 返回类型
	 * @return lua脚步执行结果
	 */
	public <R> R executeLuaScript(RedisScript<R> luaScript, List<String> keys, Object... values) {
		try {
			return (R) redisTemplate.execute(luaScript, keys, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 执行lua脚本
	 *
	 * @param luaScript  脚本内容
	 * @param returnType 返回值类型
	 * @param keys       redis键列表
	 * @param values     值列表
	 * @param <R> 返回类型
	 * @return lua脚步执行结果
	 */
	public <R> R executeLuaScript(Resource luaScript, Class<R> returnType, List<String> keys, Object... values) {
		try {
			RedisScript redisScript = RedisScript.of(luaScript, returnType);
			return (R) redisTemplate.execute(redisScript, keys, values);
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	// ===============================RedisCommand=================================

	/**
	 * 获取redis服务器时间 保证集群环境下时间一致
	 * @return Redis服务器时间戳
	 */
	public Long timeNow() {
		try {
			return redisTemplate.execute((RedisCallback<Long>) redisConnection -> redisConnection.time());
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	/**
	 * 获取redis服务器时间 保证集群环境下时间一致
	 * @param expiration 过期时间搓
	 * @return Redis服务器时间戳
	 */
	public Long period(long expiration) {
		try {
			return redisTemplate.execute((RedisCallback<Long>) redisConnection -> {
				return expiration - redisConnection.time();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long dbSize() {
		try {
			return redisTemplate.execute((RedisCallback<Long>) redisConnection -> {
				return redisConnection.dbSize();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public Long lastSave() {
		try {
			return redisTemplate.execute((RedisCallback<Long>) redisConnection -> {
				return redisConnection.lastSave();
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void bgReWriteAof() {
		try {
			redisTemplate.execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.bgReWriteAof();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void bgSave() {
		try {
			redisTemplate.execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.bgSave();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void save() {
		try {
			redisTemplate.execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.save();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void flushDb() {
		try {
			redisTemplate.execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.flushDb();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

	public void flushAll() {
		try {
			redisTemplate.execute((RedisCallback<Void>) redisConnection -> {
				redisConnection.flushAll();
				return null;
			});
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RedisOperationException(e.getMessage());
		}
	}

}
