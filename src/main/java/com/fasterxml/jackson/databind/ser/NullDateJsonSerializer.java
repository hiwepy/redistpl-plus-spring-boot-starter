package com.fasterxml.jackson.databind.ser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * 处理日期类型的null值
 */
/**
 * <p>NullDateJsonSerializer implementation.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class NullDateJsonSerializer extends JsonSerializer<Object> {

	public static final NullDateJsonSerializer INSTANCE = new NullDateJsonSerializer();
	
	@Override
	public void serialize(Object value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
			throws IOException {
		if (Objects.isNull(value)) {
			jsonGenerator.writeString(LocalDateTime.now().toString());
		}
	}
	
}
