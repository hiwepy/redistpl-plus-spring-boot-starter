package org.springframework.data.redis.core;

import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

public enum RedisKey {

	/**
	 * 用户坐标缓存
	 */
	USER_GEO_LOCATION("用户坐标", (userId)->{
		return getKeyStr(RedisKeyConstant.USER_GEO_LOCATION_KEY);
    })

	;

	private String desc;
    private Function<Object, String> function;

    RedisKey(String desc, Function<Object, String> function) {
        this.desc = desc;
        this.function = function;
    }

    public String getDesc() {
		return desc;
	}

    /**
     * 1、获取全名称key
     * @return
     */
    public String getKey() {
        return this.function.apply(null);
    }

    /**
     * 1、获取全名称key
     * @param key
     * @return
     */
    public String getKey(Object key) {
        return this.function.apply(key);
    }

    public static String REDIS_PREFIX = "rds";
    public final static String DELIMITER = ":";

    public static String getKeyStr(Object... args) {
        StringJoiner tempKey = new StringJoiner(DELIMITER);
        tempKey.add(REDIS_PREFIX);
        for (Object s : args) {
            if (Objects.isNull(s) || !StringUtils.hasText(s.toString())) {
                continue;
            }
            tempKey.add(s.toString());
        }
        return tempKey.toString();
    }

    public static String getThreadKeyStr(String prefix, Object... args) {

        StringJoiner tempKey = new StringJoiner(DELIMITER);
        tempKey.add(prefix);
        tempKey.add(String.valueOf(Thread.currentThread().getId()));
        for (Object s : args) {
            if (Objects.isNull(s) || !StringUtils.hasText(s.toString())) {
                continue;
            }
            tempKey.add(s.toString());
        }
        return tempKey.toString();
    }

    public static void main(String[] args) {
        System.out.println(getKeyStr(233,""));
    }


}
