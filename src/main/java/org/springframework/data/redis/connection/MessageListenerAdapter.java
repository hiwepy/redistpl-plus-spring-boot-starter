package org.springframework.data.redis.connection;

import org.springframework.data.redis.listener.RedisMessageListenerContainer;
/**
 * MessageListenerAdapter.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */

public interface MessageListenerAdapter extends MessageListener {

    void setMessageListenerContainer(RedisMessageListenerContainer container);

}
