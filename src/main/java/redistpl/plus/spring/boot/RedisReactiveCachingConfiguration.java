package redistpl.plus.spring.boot;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.*;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.core.ReactiveRedisOperationTemplate;
import reactor.core.publisher.Flux;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({ReactiveRedisConnectionFactory.class, ReactiveRedisTemplate.class, Flux.class })
@AutoConfigureAfter({RedisAutoConfiguration.class, RedisCachingConfiguration.class})
@ConditionalOnBean({ ReactiveRedisConnectionFactory.class })
@AutoConfigureBefore(RedisReactiveAutoConfiguration.class)
public class RedisReactiveCachingConfiguration {

	@Bean(name = "reactiveRedisTemplate")
	public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(
			ReactiveRedisConnectionFactory reactiveRedisConnectionFactory,
			Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer) {

		SerializationPair<String> stringSerializationPair = SerializationPair.fromSerializer(new RedisSerializer<String>(){

			@Override
			public byte[] serialize(String t) throws SerializationException {
				return StringRedisSerializer.UTF_8.serialize(t.toString());
			}

			@Override
			public String deserialize(byte[] bytes) throws SerializationException {
				return StringRedisSerializer.UTF_8.deserialize(bytes);
			}}
		);

		RedisSerializationContext<String, Object> serializationContext = RedisSerializationContext
				.<String, Object>newSerializationContext()
				 // 设置value的序列化规则和 key的序列化规则
				.key(stringSerializationPair)
				.value(jackson2JsonRedisSerializer)
				 // 设置hash key 和 hash value序列化模式
				.hashKey(stringSerializationPair)
				// 这个地方不可使用 json 序列化，如果使用的是ObjectRecord传输对象时，可能会有问题，会出现一个 java.lang.IllegalArgumentException: Value must not be null! 错误
				.hashValue(RedisSerializer.string())
				.build();

		return new ReactiveRedisTemplate<String, Object>(reactiveRedisConnectionFactory, serializationContext);

	}

	@Bean
	@ConditionalOnBean({ ReactiveRedisTemplate.class })
	public ReactiveRedisOperationTemplate reactiveRedisOperationTemplate(ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
		return new ReactiveRedisOperationTemplate(reactiveRedisTemplate);
	}

}
