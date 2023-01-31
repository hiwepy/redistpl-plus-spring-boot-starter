package org.springframework.data.redis.stream;

import org.springframework.data.redis.connection.stream.ObjectRecord;

public interface StreamListenerAdapter extends StreamListener<String, ObjectRecord<String, Object>>{

}
