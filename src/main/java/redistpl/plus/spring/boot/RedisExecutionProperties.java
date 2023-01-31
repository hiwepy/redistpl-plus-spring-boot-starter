package redistpl.plus.spring.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiFunction;
import java.util.function.Function;

@ConfigurationProperties(prefix = "spring.redis.executor")
public class RedisExecutionProperties {

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
    private Pool stream = new Pool();

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

    public void setStream(Pool stream) {
        this.stream = stream;
    }

    public Pool getStream() {
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

        private String threadNamePrefix = "redis-execution-";

        private boolean daemon = false;

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
