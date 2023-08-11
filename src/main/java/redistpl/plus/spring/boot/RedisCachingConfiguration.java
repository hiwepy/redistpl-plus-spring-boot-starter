package redistpl.plus.spring.boot;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.ConfigFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.RedisBeanSerializerModifier;
import hitool.core.lang3.time.DateFormats;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.annotation.RedisChannelTopic;
import org.springframework.data.redis.annotation.RedisPatternTopic;
import org.springframework.data.redis.annotation.RedisStreamConsumer;
import org.springframework.data.redis.connection.MessageListenerAdapter;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamListenerAdapter;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.*;
import com.fasterxml.jackson.databind.json.JsonMapperBuilderCustomizer;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
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
@EnableConfigurationProperties({RedisProperties.class, RedisThreadPoolProperties.class})
public class RedisCachingConfiguration extends CachingConfigurerSupport {

	private static final Map<ConfigFeature, Boolean> FEATURE_DEFAULTS;

	static {
		Map<ConfigFeature, Boolean> featureDefaults = new HashMap<>();
		featureDefaults.put(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		featureDefaults.put(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false);
		featureDefaults.put(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		featureDefaults.put(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		featureDefaults.put(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, true);
		// 使用getter取代setter探测属性，这是针对集合类型，可以直接修改集合的属性
		featureDefaults.put(MapperFeature.USE_GETTERS_AS_SETTERS, true);

		FEATURE_DEFAULTS = Collections.unmodifiableMap(featureDefaults);
	}

	/**
	 * 单独初始化ObjectMapper，不使用全局对象，保持缓存序列化的独立配置
	 * @return ObjectMapper
	 */
	protected ObjectMapper objectMapper(List<JsonMapperBuilderCustomizer> customizers) {

		JsonMapper.Builder builder = JsonMapper.builder()
				// 指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会跑出异常
				.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL)
				.defaultDateFormat(new SimpleDateFormat(DateFormats.DATE_LONGFORMAT))
				// 指定要序列化的域，field,get和set,以及修饰符范围，ANY是都有包括private和public
				.visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
				.serializationInclusion(JsonInclude.Include.NON_NULL)
				// 为objectMapper注册一个带有SerializerModifier的Factory
				.serializerFactory(BeanSerializerFactory.instance.withSerializerModifier(new RedisBeanSerializerModifier()));

		for (JsonMapperBuilderCustomizer customizer : customizers) {
			customizer.customize(builder);
		}

		return builder.build();
	}

	@Bean(name = "redisTemplate")
	public RedisTemplate<String, Object> redisTemplate(ObjectProvider<RedisConnectionFactory> redisConnectionFactoryProvider,
													   ObjectProvider<JsonMapperBuilderCustomizer> customizerProvider) {

		// 1、获取自定义的JsonMapperBuilderCustomizer
		List<JsonMapperBuilderCustomizer> customizers = customizerProvider.orderedStream().collect(Collectors.toList());
		// 2、初始化 ObjectMapper
		ObjectMapper objectMapper = this.objectMapper(customizers);
		// 3、初始化 Jackson2JsonRedisSerializer<Object>，用于RedisTemplate的序列化和反序列化
		//GenericJackson2JsonRedisSerializer genericJackson2JsonRedisSerializer = new GenericJackson2JsonRedisSerializer(objectMapperProvider.getIfAvailable());
		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
		jackson2JsonRedisSerializer.setObjectMapper(objectMapper);

		// 4、初始化 RedisTemplate<String, Object> 对象
		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		// 4.1、设置连接工厂
		redisTemplate.setConnectionFactory(redisConnectionFactoryProvider.getIfAvailable());

		// 5、使用StringRedisSerializer来序列化和反序列化redis的key值
		redisTemplate.setKeySerializer(RedisSerializer.string());
		// 6、用Jackson2JsonRedisSerializer来序列化和反序列化redis的value值
		redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);

		// 7、设置hash key 序列化模式
		redisTemplate.setHashKeySerializer(new GenericToStringSerializer<>(Object.class));
		// 8、设置hash value序列化模式
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
																	   ObjectProvider<MeterRegistry> registryProvider,
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
		container.setTaskExecutor(redisThreadPoolTaskExecutor(redisThreadPoolProperties.getListener(), registryProvider.getIfAvailable(), "redis.listener.thread-pool"));
		// 设置Redis频道订阅的任务执行器
		container.setSubscriptionExecutor(redisThreadPoolTaskExecutor(redisThreadPoolProperties.getSubscription(), registryProvider.getIfAvailable(), "redis.subscription.thread-pool"));
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
			ObjectProvider<MeterRegistry> registryProvider,
			RedisThreadPoolProperties redisThreadPoolProperties) throws UnknownHostException {

		RedisThreadPoolProperties.StreamPool streamPool = redisThreadPoolProperties.getStream();
		ThreadPoolTaskExecutor executor = redisThreadPoolTaskExecutor(streamPool, registryProvider.getIfAvailable(), "redis.stream.thread-pool");

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
	protected ThreadPoolTaskExecutor redisThreadPoolTaskExecutor(RedisThreadPoolProperties.Pool pool, MeterRegistry registry, String metricPrefix){

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
		/**
		 * 监控线程池
		 * 1.线程池的状态
		 * 2.线程池的队列
		 * 3.线程池的拒绝策略
		 */
		ExecutorServiceMetrics.monitor(registry, executor, pool.getThreadNamePrefix(),  metricPrefix);
		return executor;
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(JsonMapper.class)
	@EnableConfigurationProperties(RedisJacksonProperties.class)
	static class JsonMapperBuilderCustomizerConfiguration {

		@Bean
		RedisCachingConfiguration.JsonMapperBuilderCustomizerConfiguration.StandardJsonMapperBuilderCustomizer standardJsonMapperBuilderCustomizer(
				ApplicationContext applicationContext, RedisJacksonProperties jacksonProperties) {
			return new RedisCachingConfiguration.JsonMapperBuilderCustomizerConfiguration.StandardJsonMapperBuilderCustomizer(applicationContext, jacksonProperties);
		}

		static final class StandardJsonMapperBuilderCustomizer
				implements JsonMapperBuilderCustomizer, Ordered {

			private final ApplicationContext applicationContext;

			private final RedisJacksonProperties jacksonProperties;

			StandardJsonMapperBuilderCustomizer(ApplicationContext applicationContext,
												RedisJacksonProperties jacksonProperties) {
				this.applicationContext = applicationContext;
				this.jacksonProperties = jacksonProperties;
			}

			@Override
			public int getOrder() {
				return 0;
			}

			@Override
			public void customize(JsonMapper.Builder builder) {

				if (this.jacksonProperties.getDefaultPropertyInclusion() != null) {
					builder.serializationInclusion(this.jacksonProperties.getDefaultPropertyInclusion());
				}
				if (this.jacksonProperties.getTimeZone() != null) {
					builder.defaultTimeZone(this.jacksonProperties.getTimeZone());
				}
				configureFeatures(builder, FEATURE_DEFAULTS);
				configureVisibility(builder, this.jacksonProperties.getVisibility());
				configureFeatures(builder, this.jacksonProperties.getDeserialization());
				configureFeatures(builder, this.jacksonProperties.getSerialization());
				configureFeatures(builder, this.jacksonProperties.getMapper());
				configureFeatures(builder, this.jacksonProperties.getParser());
				configureFeatures(builder, this.jacksonProperties.getGenerator());
				configureDateFormat(builder);
				configurePropertyNamingStrategy(builder);
				configureModules(builder);
				configureLocale(builder);
			}

			private void configureFeatures(JsonMapper.Builder builder, Map<?, Boolean> features) {
				features.forEach((feature, value) -> {
					if (value != null) {
						if (value) {
							if(feature instanceof DeserializationFeature) {
								builder.enable((DeserializationFeature) feature);
							} else if(feature instanceof SerializationFeature) {
								builder.enable((SerializationFeature) feature);
							} else if(feature instanceof MapperFeature) {
								builder.enable((MapperFeature) feature);
							} else if(feature instanceof JsonParser.Feature) {
								builder.enable((JsonParser.Feature) feature);
							} else if(feature instanceof JsonGenerator.Feature) {
								builder.enable((JsonGenerator.Feature) feature);
							}
						}
						else {
							if(feature instanceof DeserializationFeature) {
								builder.disable((DeserializationFeature) feature);
							} else if(feature instanceof SerializationFeature) {
								builder.disable((SerializationFeature) feature);
							} else if(feature instanceof MapperFeature) {
								builder.disable((MapperFeature) feature);
							} else if(feature instanceof JsonParser.Feature) {
								builder.disable((JsonParser.Feature) feature);
							} else if(feature instanceof JsonGenerator.Feature) {
								builder.disable((JsonGenerator.Feature) feature);
							}
						}
					}
				});
			}

			private void configureVisibility(JsonMapper.Builder builder,
											 Map<PropertyAccessor, JsonAutoDetect.Visibility> visibilities) {
				visibilities.forEach(builder::visibility);
			}

			private void configureDateFormat(JsonMapper.Builder builder) {
				// We support a fully qualified class name extending DateFormat or a date
				// pattern string value
				String dateFormat = Optional.ofNullable(this.jacksonProperties.getDateFormat()).orElse(DateFormats.DATE_LONGFORMAT);
				if (dateFormat != null) {
					try {
						Class<?> dateFormatClass = ClassUtils.forName(dateFormat, null);
						builder.defaultDateFormat((DateFormat) BeanUtils.instantiateClass(dateFormatClass));
					}
					catch (ClassNotFoundException ex) {
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
						// Since Jackson 2.6.3 we always need to set a TimeZone (see
						// gh-4170). If none in our properties fallback to the Jackson's
						// default
						TimeZone timeZone = this.jacksonProperties.getTimeZone();
						if (timeZone == null) {
							timeZone = new ObjectMapper().getSerializationConfig().getTimeZone();
						}
						simpleDateFormat.setTimeZone(timeZone);
						builder.defaultDateFormat(simpleDateFormat);
					}
				}
			}

			private void configurePropertyNamingStrategy(JsonMapper.Builder builder) {
				// We support a fully qualified class name extending Jackson's
				// PropertyNamingStrategy or a string value corresponding to the constant
				// names in PropertyNamingStrategy which hold default provided
				// implementations
				String strategy = this.jacksonProperties.getPropertyNamingStrategy();
				if (strategy != null) {
					try {
						configurePropertyNamingStrategyClass(builder, ClassUtils.forName(strategy, null));
					}
					catch (ClassNotFoundException ex) {
						configurePropertyNamingStrategyField(builder, strategy);
					}
				}
			}

			private void configurePropertyNamingStrategyClass(JsonMapper.Builder builder,
															  Class<?> propertyNamingStrategyClass) {
				builder.propertyNamingStrategy((PropertyNamingStrategy) BeanUtils.instantiateClass(propertyNamingStrategyClass));
			}

			private void configurePropertyNamingStrategyField(JsonMapper.Builder builder, String fieldName) {
				// Find the field (this way we automatically support new constants
				// that may be added by Jackson in the future)
				Field field = ReflectionUtils.findField(PropertyNamingStrategy.class, fieldName,
						PropertyNamingStrategy.class);
				Assert.notNull(field, () -> "Constant named '" + fieldName + "' not found on "
						+ PropertyNamingStrategy.class.getName());
				try {
					builder.propertyNamingStrategy((PropertyNamingStrategy) field.get(null));
				}
				catch (Exception ex) {
					throw new IllegalStateException(ex);
				}
			}

			private void configureModules(JsonMapper.Builder builder) {
				Collection<Module> moduleBeans = getBeans(this.applicationContext, Module.class);
				builder.addModules(moduleBeans.toArray(new Module[0]));
			}

			private void configureLocale(JsonMapper.Builder builder) {
				Locale locale = this.jacksonProperties.getLocale();
				if (locale != null) {
					builder.defaultLocale(locale);
				}
			}

			private static <T> Collection<T> getBeans(ListableBeanFactory beanFactory, Class<T> type) {
				return BeanFactoryUtils.beansOfTypeIncludingAncestors(beanFactory, type).values();
			}

		}

	}
}
