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
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.couchbase.CouchbaseAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.annotation.RedisStreamConsumer;
import org.springframework.data.redis.connection.MessageListenerAdapter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.annotation.RedisChannelTopic;
import org.springframework.data.redis.annotation.RedisPatternTopic;
import org.springframework.data.redis.stream.StreamListenerAdapter;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Reids 相关bean的配置
 * https://www.cnblogs.com/liuyp-ken/p/10538658.html
 * https://www.cnblogs.com/aoeiuv/p/6760798.html
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(RedisOperations.class)
@AutoConfigureAfter({CacheAutoConfiguration.class})
@AutoConfigureBefore(RedisAutoConfiguration.class)
@EnableConfigurationProperties(RedisThreadPoolProperties.class)
public class RedisCachingConfiguration extends CachingConfigurerSupport {

	@Bean
	public Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer(ObjectProvider<ObjectMapper> objectMapperProvider) {

		// 使用Jackson2JsonRedisSerialize 替换默认序列化
		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);

		ObjectMapper objectMapper = objectMapperProvider.getIfAvailable(() -> {
			return JsonMapper.builder()
					// 指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会跑出异常
					.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL)
					.enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
					.enable(MapperFeature.USE_GETTERS_AS_SETTERS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
					.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
					// 指定要序列化的域，field,get和set,以及修饰符范围，ANY是都有包括private和public
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

		// 设置value的序列化规则和 key的序列化规则

		// 使用StringRedisSerializer来序列化和反序列化redis的key值
		redisTemplate.setKeySerializer(RedisSerializer.string());
		// 值采用json序列化
		redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);

		// 设置hash key 和value序列化模式
		redisTemplate.setHashKeySerializer(RedisSerializer.string());
		// 这个地方不可使用 json 序列化，如果使用的是ObjectRecord传输对象时，可能会有问题，会出现一个 java.lang.IllegalArgumentException: Value must not be null! 错误
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

	@ConditionalOnMissingBean
	@Bean(initMethod = "start", destroyMethod = "stop")
	public RedisMessageListenerContainer redisMessageListenerContainer(ObjectProvider<RedisConnectionFactory> redisConnectionFactoryProvider,
																	   ObjectProvider<MessageListenerAdapter> messageListenerProvider,
																	   RedisThreadPoolProperties redisThreadPoolProperties) {
		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(redisConnectionFactoryProvider.getIfAvailable());
		// 订阅多个频道
		List<MessageListenerAdapter> messageListenerAdapters = messageListenerProvider.orderedStream().collect(Collectors.toList());
		if (!CollectionUtils.isEmpty(messageListenerAdapters)) {
			for (MessageListenerAdapter messageListener : messageListenerAdapters) {
				// 查找注解
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
		// 序列化对象（特别注意：发布的时候需要设置序列化；订阅方也需要设置序列化）
		container.setTopicSerializer(RedisSerializer.string());
		// 设置接收消息时用于运行消息侦听器的任务执行器
		container.setTaskExecutor(redisThreadPoolTaskExecutor(redisThreadPoolProperties.getListener()));
		// 设置Redis频道订阅的任务执行器
		container.setSubscriptionExecutor(redisThreadPoolTaskExecutor(redisThreadPoolProperties.getSubscription()));
		return container;
	}

	/**
	 * 可以同时支持 独立消费 和 消费者组 消费
	 * <p>
	 * 可以支持动态的 增加和删除 消费者
	 * <p>
	 * 消费组需要预先创建出来
	 *
	 * @return StreamMessageListenerContainer
	 */
	@ConditionalOnMissingBean
	@Bean(initMethod = "start", destroyMethod = "stop")
	public StreamMessageListenerContainer<String, ObjectRecord<String, Object>> streamMessageListenerContainer(
			ObjectProvider<StringRedisTemplate> stringRedisTemplateProvider,
			ObjectProvider<RedisConnectionFactory> redisConnectionFactoryProvider,
			ObjectProvider<StreamListenerAdapter> streamMessageListenerProvider,
			ObjectProvider<StreamMessageErrorHandler> streamMessageErrorHandlerProvider,
			RedisThreadPoolProperties redisThreadPoolProperties) throws UnknownHostException {

		RedisThreadPoolProperties.StreamPool streamPool = redisThreadPoolProperties.getStream();
		ThreadPoolTaskExecutor executor = redisThreadPoolTaskExecutor(streamPool);

		StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, Object>> options =
				StreamMessageListenerContainer.StreamMessageListenerContainerOptions
						.builder()
						// 一次最多获取多少条消息
						.batchSize(streamPool.getBatchSize())
						// 运行 Stream 的 poll task
						.executor(executor)
						// 可以理解为 Stream Key 的序列化方式
						.keySerializer(RedisSerializer.string())
						// 可以理解为 Stream 后方的字段的 key 的序列化方式
						.hashKeySerializer(RedisSerializer.string())
						// 可以理解为 Stream 后方的字段的 value 的序列化方式
						.hashValueSerializer(RedisSerializer.string())
						// Stream 中没有消息时，阻塞多长时间，需要比 `spring.redis.timeout` 的时间小
						.pollTimeout(streamPool.getPollTimeout())
						// ObjectRecord 时，将 对象的 filed 和 value 转换成一个 Map 比如：将Book对象转换成map
						.objectMapper(new ObjectHashMapper())
						// 获取消息的过程或获取到消息给具体的消息者处理的过程中，发生了异常的处理
						.errorHandler(new NestedErrorHandler(streamMessageErrorHandlerProvider.orderedStream().collect(Collectors.toList())))
						// 将发送到Stream中的Record转换成ObjectRecord，转换成具体的类型是这个地方指定的类型
						.targetType(Object.class)
						.build();

		StreamMessageListenerContainer<String, ObjectRecord<String, Object>> streamMessageListenerContainer =
				StreamMessageListenerContainer.create(redisConnectionFactoryProvider.getIfAvailable(), options);

		// 多个消费者
		List<StreamListenerAdapter> messageListenerAdapters = streamMessageListenerProvider.orderedStream().collect(Collectors.toList());
		if (!CollectionUtils.isEmpty(messageListenerAdapters)) {
			for (StreamListenerAdapter messageListener : messageListenerAdapters) {
				// 查找注解
				RedisStreamConsumer consumer = AnnotationUtils.findAnnotation(messageListener.getClass(), RedisStreamConsumer.class);
				if (Objects.nonNull(consumer) && StringUtils.hasText(consumer.value())){
					// 获取消费信息配置
					String streamKey = consumer.streamKey();
					String groupName = consumer.groupName();
					ReadOffset readOffset =	StringUtils.hasText(consumer.readOffset()) ? ReadOffset.from(consumer.readOffset()) : ReadOffset.lastConsumed();
					// stream 消费开始位置
					StreamOffset<String> streamOffset = StreamOffset.create(streamKey,readOffset);
					// 默认创建 消费组
					stringRedisTemplateProvider.getIfAvailable().opsForStream().createGroup(streamKey, groupName);
					// 根据不同条件注册消费者
					if(consumer.autoAck() && StringUtils.hasText(groupName)){
						String consumerName = StringUtils.hasText(consumer.consumerName()) ? consumer.consumerName() : InetAddress.getLocalHost().getHostName();
						streamMessageListenerContainer.receiveAutoAck(Consumer.from(groupName, consumerName),
								streamOffset, messageListener);
					} else if(!consumer.autoAck() && StringUtils.hasText(groupName)){
						String consumerName = StringUtils.hasText(consumer.consumerName()) ? consumer.consumerName() : InetAddress.getLocalHost().getHostName();
						streamMessageListenerContainer.receive(Consumer.from(groupName, consumerName),
								streamOffset, messageListener);
					} else {
						streamMessageListenerContainer.receive(streamOffset, messageListener);
					}
				}
			}
		}
		return streamMessageListenerContainer;
	}

	/**
	 * 构建线程池
	 * @param pool
	 * @return
	 */
	protected ThreadPoolTaskExecutor redisThreadPoolTaskExecutor(RedisThreadPoolProperties.Pool pool){

  		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(pool.getCoreSize());
		executor.setMaxPoolSize(pool.getMaxSize());
		executor.setQueueCapacity(pool.getQueueCapacity());
		executor.setKeepAliveSeconds(Long.valueOf(pool.getKeepAlive().getSeconds()).intValue());
		executor.setThreadNamePrefix(pool.getThreadNamePrefix());
		executor.setDaemon(pool.isDaemon());
		/**
		 * 拒绝处理策略
		 * CallerRunsPolicy()：交由调用方线程运行，比如 main 线程。
		 * AbortPolicy()：直接抛出异常。
		 * DiscardPolicy()：直接丢弃。
		 * DiscardOldestPolicy()：丢弃队列中最老的任务。
		 */
		executor.setRejectedExecutionHandler(pool.getRejectedPolicy().getRejectedExecutionHandler());
		// 线程初始化
		executor.initialize();
		return executor;
	}

}
