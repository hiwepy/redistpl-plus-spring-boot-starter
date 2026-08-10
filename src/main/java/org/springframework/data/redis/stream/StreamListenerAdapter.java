package org.springframework.data.redis.stream;

import org.springframework.data.redis.connection.stream.ObjectRecord;
/**
 * StreamListenerAdapter.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */

public interface StreamListenerAdapter extends StreamListener<String, ObjectRecord<String, Object>>{

}
