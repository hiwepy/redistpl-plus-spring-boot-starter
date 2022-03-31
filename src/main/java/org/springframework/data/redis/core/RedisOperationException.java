package org.springframework.data.redis.core;

import org.springframework.dao.NonTransientDataAccessException;

/**
 * @author wandl
 */
@SuppressWarnings("serial")
public class RedisOperationException extends NonTransientDataAccessException {

	public RedisOperationException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public RedisOperationException(String msg) {
		super(msg);
	}

}
