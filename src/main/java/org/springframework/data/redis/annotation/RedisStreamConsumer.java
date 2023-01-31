package org.springframework.data.redis.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RedisStreamConsumer {

	String streamKey();
	String groupName();
	String consumerName();
	String hostName();
	boolean autoAck() default true;
	String value();

}
