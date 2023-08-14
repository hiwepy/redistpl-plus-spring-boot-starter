package org.springframework.data.redis.util;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.json.JsonMapperBuilderCustomizer;
import hitool.core.lang3.time.DateFormats;

import java.text.SimpleDateFormat;
import java.util.List;

public class ObjectMappers {

    /**
     * 单独初始化ObjectMapper，不使用全局对象，保持缓存序列化的独立配置
     * @return ObjectMapper
     */
    public static ObjectMapper defaultObjectMapper(List<JsonMapperBuilderCustomizer> customizers) {

        JsonMapper.Builder builder = JsonMapper.builder()
                // 指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会跑出异常
                //.activateDefaultTyping(LaissezFaireSubTypeValidator.instance, ObjectMapper.DefaultTyping.NON_FINAL)
                .defaultDateFormat(new SimpleDateFormat(DateFormats.DATE_LONGFORMAT))
                // 指定要序列化的域，field,get和set,以及修饰符范围，ANY是都有包括private和public
                .visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
                .serializationInclusion(JsonInclude.Include.NON_NULL)
                // 为objectMapper注册一个带有SerializerModifier的Factory
                //.serializerFactory(BeanSerializerFactory.instance.withSerializerModifier(new RedisBeanSerializerModifier()))
                ;

        for (JsonMapperBuilderCustomizer customizer : customizers) {
            customizer.customize(builder);
        }

        return builder.build();
    }

}
