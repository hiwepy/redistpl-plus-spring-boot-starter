package redistpl.plus.spring.boot;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.connection.MessageListenerAdapter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperationTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.annotation.RedisChannelTopic;
import org.springframework.data.redis.annotation.RedisPatternTopic;
import org.springframework.data.redis.core.GeoTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * Reids ??????bean?????????
 * https://www.cnblogs.com/liuyp-ken/p/10538658.html
 * https://www.cnblogs.com/aoeiuv/p/6760798.html
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(RedisOperations.class)
@EnableCaching(proxyTargetClass = true)
@EnableConfigurationProperties(RedisExecutionProperties.class)
public class RedisCachingConfiguration extends CachingConfigurerSupport {

	@Bean
	public Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer(ObjectProvider<ObjectMapper> objectMapperProvider) {

		// ??????Jackson2JsonRedisSerialize ?????????????????????
		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);

		ObjectMapper objectMapper = objectMapperProvider.getIfAvailable(() -> {
			return JsonMapper.builder()
					// ????????????????????????????????????????????????final????????????final?????????????????????String,Integer??????????????????
					.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL)
					.enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
					.enable(MapperFeature.USE_GETTERS_AS_SETTERS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
					.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
					// ???????????????????????????field,get???set,????????????????????????ANY???????????????private???public
					.visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
					.serializationInclusion(JsonInclude.Include.NON_NULL)
					.build();
		});
		jackson2JsonRedisSerializer.setObjectMapper(objectMapper);
		return jackson2JsonRedisSerializer;
	}

	@Bean(name = "redisTemplate")
	public RedisTemplate<String, Object> redisTemplate(ObjectProvider<RedisConnectionFactory> redisConnectionFactoryProvider,
			Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer) {
		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(redisConnectionFactoryProvider.getIfAvailable());

		// ??????value????????????????????? key??????????????????

		// ??????StringRedisSerializer???????????????????????????redis???key???
		redisTemplate.setKeySerializer(RedisSerializer.string());
		// ?????????json?????????
		redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);

		// ??????hash key ???value???????????????
		redisTemplate.setHashKeySerializer(RedisSerializer.string());
		redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);
		redisTemplate.afterPropertiesSet();

		return redisTemplate;
	}

	@Bean
	public StringRedisTemplate stringRedisTemplate(ObjectProvider<RedisConnectionFactory> redisConnectionFactoryProvider) {
		StringRedisTemplate redisTemplate = new StringRedisTemplate();
		redisTemplate.setConnectionFactory(redisConnectionFactoryProvider.getIfAvailable());
		redisTemplate.setEnableTransactionSupport(true);
		return redisTemplate;
	}

	@Bean
	@Order(1)
	public RedisOperationTemplate redisOperationTemplate(RedisTemplate<String, Object> redisTemplate) {
		return new RedisOperationTemplate(redisTemplate);
	}

	@Bean
	public GeoTemplate geoTemplate(RedisTemplate<String, Object> redisTemplate) {
		return new GeoTemplate(redisTemplate);
	}

	@Bean
	public RedisMessageListenerContainer redisMessageListenerContainer(ObjectProvider<RedisConnectionFactory> redisConnectionFactoryProvider,
																	   ObjectProvider<MessageListenerAdapter> messageListenerProvider,
																	   RedisExecutionProperties redisExecutionProperties) {
		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(redisConnectionFactoryProvider.getIfAvailable());
		// ??????????????????
		List<MessageListenerAdapter> messageListenerAdapters = messageListenerProvider.orderedStream().collect(Collectors.toList());
		if (!CollectionUtils.isEmpty(messageListenerAdapters)) {
			for (MessageListenerAdapter messageListener : messageListenerAdapters) {
				// ????????????
				RedisChannelTopic channel = AnnotationUtils.findAnnotation(messageListener.getClass(), RedisChannelTopic.class);
				if (Objects.nonNull(channel) && StringUtils.hasText(channel.value())){
					container.addMessageListener(messageListener, new ChannelTopic(channel.value()));
					continue;
				}
				RedisPatternTopic pattern = AnnotationUtils.findAnnotation(messageListener.getClass(), RedisPatternTopic.class);
				if (Objects.nonNull(pattern) && StringUtils.hasText(pattern.value())){
					container.addMessageListener(messageListener, new PatternTopic(pattern.value()));
				}
			}
		}
		// ????????????????????????????????????????????????????????????????????????????????????????????????????????????
		container.setTopicSerializer(RedisSerializer.string());
		// ??????????????????????????????????????????????????????????????????
		container.setTaskExecutor(redisThreadPoolTaskExecutor(redisExecutionProperties.getListener()));
		// ??????Redis??????????????????????????????
		container.setSubscriptionExecutor(redisThreadPoolTaskExecutor(redisExecutionProperties.getSubscription()));
		return container;
	}

	protected ThreadPoolTaskExecutor redisThreadPoolTaskExecutor(RedisExecutionProperties.Pool pool){
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		// ???????????????
		executor.setCorePoolSize(pool.getCoreSize());
		// ???????????????
		executor.setMaxPoolSize(pool.getMaxSize());
		// ?????????????????????
		executor.setQueueCapacity(pool.getQueueCapacity());
		// ??????????????????
		executor.setKeepAliveSeconds(Long.valueOf(pool.getKeepAlive().getSeconds()).intValue());
		// ???????????????
		executor.setThreadNamePrefix(pool.getThreadNamePrefix());
		/**
		 * ??????????????????
		 * CallerRunsPolicy()??????????????????????????????????????? main ?????????
		 * AbortPolicy()????????????????????????
		 * DiscardPolicy()??????????????????
		 * DiscardOldestPolicy()????????????????????????????????????
		 */
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
		// ???????????????
		executor.initialize();
		return executor;
	}

}
