package redistpl.plus.spring.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

@ConfigurationProperties(prefix = "spring.redis.thread-pool")
public class RedisThreadPoolProperties {

    /**
     * listener pool configuration.
     */
    private Pool listener = new Pool();

    /**
     * subscription pool configuration.
     */
    private Pool subscription = new Pool();

    /**
     * stream pool configuration.
     */
    private StreamPool stream = new StreamPool();

    public void setListener(Pool listener) {
        this.listener = listener;
    }

    public Pool getListener() {
        return listener;
    }

    public void setSubscription(Pool subscription) {
        this.subscription = subscription;
    }

    public Pool getSubscription() {
        return subscription;
    }

    public void setStream(StreamPool stream) {
        this.stream = stream;
    }

    public StreamPool getStream() {
        return stream;
    }

    /**
     * Pool properties.
     */
    public static class Pool {

        /**
         * Set the ThreadPoolExecutor's core pool size. Default is 1.
         * positive.
         */
        private int coreSize = 1;

        /**
         * Set the ThreadPoolExecutor's maximum pool size. Default is the number of Processor.
         */
        private int maxSize = Runtime.getRuntime().availableProcessors();

        /**
         * Set the capacity for the ThreadPoolExecutor's BlockingQueue. Default is Integer.MAX_VALUE.
         * Any positive value will lead to a LinkedBlockingQueue instance; any other value will lead to a SynchronousQueue instance.
         */
        private int queueCapacity = Integer.MAX_VALUE;

        /**
         * Set the ThreadPoolExecutor's keep-alive time. Default is 60 seconds.
         */
        private Duration keepAlive = Duration.ofSeconds(60);

        /**
         * Specify whether to allow core threads to time out. This enables dynamic
         * growing and shrinking even in combination with a non-zero queue (since
         * the max pool size will only grow once the queue is full).
         * <p>Default is "false".
         */
        private boolean allowCoreThreadTimeOut = false;

        /**
         * Specify the prefix to use for the names of newly created threads.
         * Default is "RedisAsyncTaskExecutor-".
         */
        private String threadNamePrefix = "RedisAsyncTaskExecutor-";

        /**
         * Set whether this factory is supposed to create daemon threads,
         * just executing as long as the application itself is running.
         * <p>Default is "false": Concrete factories usually support explicit cancelling.
         * Hence, if the application shuts down, Runnables will by default finish their
         * execution.
         * <p>Specify "true" for eager shutdown of threads which still actively execute
         * a {@link Runnable} at the time that the application itself shuts down.
         */
        private boolean daemon = false;

        /**
         * Set the Rejected Policy to use for the ExecutorService.
         * Default is the ExecutorService's default abort policy.
         * @see java.util.concurrent.ThreadPoolExecutor.AbortPolicy
         */
        private RejectedPolicy rejectedPolicy = RejectedPolicy.AbortPolicy;

        public void setCoreSize(int coreSize) {
            this.coreSize = coreSize;
        }

        public int getCoreSize() {
            return coreSize;
        }

        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        public int getMaxSize() {
            return maxSize;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setKeepAlive(Duration keepAlive) {
            this.keepAlive = keepAlive;
        }

        public Duration getKeepAlive() {
            return keepAlive;
        }

        public void setAllowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) {
            this.allowCoreThreadTimeOut = allowCoreThreadTimeOut;
        }

        public boolean isAllowCoreThreadTimeOut() {
            return allowCoreThreadTimeOut;
        }

        public void setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
        }

        public String getThreadNamePrefix() {
            return threadNamePrefix;
        }

        public void setDaemon(boolean daemon) {
            this.daemon = daemon;
        }

        public boolean isDaemon() {
            return daemon;
        }

        public RejectedPolicy getRejectedPolicy() {
            return rejectedPolicy;
        }

        public void setRejectedPolicy(RejectedPolicy rejectedPolicy) {
            this.rejectedPolicy = rejectedPolicy;
        }

    }


    /**
     * Pool properties.
     */
    public static class StreamPool extends Pool {

        /**
         * The batch size for the COUNT option during reading.
         */
        private Integer batchSize = 1;

        /**
         * Stream 中没有消息时，阻塞多长时间，需要比 `spring.redis.timeout` 的时间小
         * The poll timeout for the {@code BLOCK} option during reading.
         */
        private Duration pollTimeout = Duration.ofSeconds(2);

        private ReadOffsetPolicy readOffset = ReadOffsetPolicy.lastConsumed;

        public void setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setReadOffset(ReadOffsetPolicy readOffset) {
            this.readOffset = readOffset;
        }

        public ReadOffsetPolicy getReadOffset() {
            return readOffset;
        }
    }

    public enum ReadOffsetPolicy {

        from((offset) -> {
            return ReadOffset.from(offset);
        }),
        latest((offset) -> {
            return ReadOffset.latest();
        }),
        lastConsumed((offset) -> {
            return ReadOffset.lastConsumed();
        });

        private Function<String, ReadOffset> function;

        private ReadOffsetPolicy(Function<String, ReadOffset> function) {
            this.function = function;
        }

        public ReadOffset getReadOffset(){
            return this.function.apply(">");
        }
        public ReadOffset getReadOffset(String offset){
            return this.function.apply(offset);
        }

    }

    /**
     * 拒绝处理策略
     * CallerRunsPolicy()：交由调用方线程运行，比如 main 线程。
     * AbortPolicy()：直接抛出异常。
     * DiscardPolicy()：直接丢弃。
     * DiscardOldestPolicy()：丢弃队列中最老的任务。
     */
    public enum RejectedPolicy {

        AbortPolicy((e) -> {
            return new ThreadPoolExecutor.AbortPolicy();
        }),
        CallerRunsPolicy((e) -> {
            return new ThreadPoolExecutor.CallerRunsPolicy();
        }),
        DiscardPolicy((e) -> {
            return new ThreadPoolExecutor.DiscardPolicy();
        }),
        DiscardOldestPolicy((e) -> {
            return new ThreadPoolExecutor.DiscardOldestPolicy();
        });

        private Function<Object, RejectedExecutionHandler> function;

        private RejectedPolicy(Function<Object, RejectedExecutionHandler> function) {
            this.function = function;
        }

        public RejectedExecutionHandler getRejectedExecutionHandler(){
            return this.function.apply(null);
        }

    }
}
