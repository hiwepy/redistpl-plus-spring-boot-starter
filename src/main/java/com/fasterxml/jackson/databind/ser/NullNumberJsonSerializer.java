package com.fasterxml.jackson.databind.ser;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Objects;

/**
 * 处理数值类型的null值
 */
/**
 * <p>NullNumberJsonSerializer implementation.</p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class NullNumberJsonSerializer extends JsonSerializer<Object> {

	public static final NullNumberJsonSerializer INSTANCE = new NullNumberJsonSerializer();
	
	@Override
	public void serialize(Object value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
			throws IOException {
		if (Objects.isNull(value)) {
			jsonGenerator.writeNumber(0);
		}
	}
	
}
