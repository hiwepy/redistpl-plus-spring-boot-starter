package org.springframework.data.redis.stream;

import org.springframework.data.redis.connection.stream.ObjectRecord;
/**
 * StreamListenerAdapter.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */

public interface StreamListenerAdapter extends StreamListener<String, ObjectRecord<String, Object>>{

}
