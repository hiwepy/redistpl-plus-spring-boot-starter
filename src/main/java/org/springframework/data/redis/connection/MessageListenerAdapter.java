package org.springframework.data.redis.connection;

import org.springframework.data.redis.listener.RedisMessageListenerContainer;

public interface MessageListenerAdapter extends MessageListener {

    void setMessageListenerContainer(RedisMessageListenerContainer container);

}
