package org.springframework.data.redis.core;

import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.StringJoiner;

public abstract class RedisKeyConstant {

	/**
	 * 用户坐标缓存
	 */
	public final static String USER_GEO_LOCATION_KEY = "user:geo:location";

}
