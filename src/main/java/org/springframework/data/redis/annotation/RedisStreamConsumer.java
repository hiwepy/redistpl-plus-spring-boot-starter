package org.springframework.data.redis.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
/**
 * <p>RedisStreamConsumer implementation.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public @interface RedisStreamConsumer {

	String streamKey();
	String groupName();
	String consumerName();
	String readOffset();
	boolean autoAck() default true;
	String value();

}
