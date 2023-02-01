package org.springframework.data.redis.annotation;

import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import redistpl.plus.spring.boot.RedisThreadPoolProperties;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RedisStreamConsumer {

	String streamKey();
	String groupName();
	String consumerName();
	String readOffset();
	boolean autoAck() default true;
	String value();

}
