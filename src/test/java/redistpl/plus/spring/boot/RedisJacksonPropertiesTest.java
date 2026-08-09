package redistpl.plus.spring.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Tests for {@link RedisJacksonProperties}.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 */
class RedisJacksonPropertiesTest {

    @Test
    void defaultValues() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        assertThat(props.getDateFormat()).isNull();
        assertThat(props.getPropertyNamingStrategy()).isNull();
        assertThat(props.getVisibility()).isEmpty();
        assertThat(props.getSerialization()).isEmpty();
        assertThat(props.getDeserialization()).isEmpty();
        assertThat(props.getMapper()).isEmpty();
        assertThat(props.getParser()).isEmpty();
        assertThat(props.getGenerator()).isEmpty();
        assertThat(props.getDefaultPropertyInclusion()).isNull();
        assertThat(props.getTimeZone()).isNull();
        assertThat(props.getLocale()).isNull();
    }

    @Test
    void setAndGetDateFormat() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.setDateFormat("yyyy-MM-dd HH:mm:ss");
        assertThat(props.getDateFormat()).isEqualTo("yyyy-MM-dd HH:mm:ss");
    }

    @Test
    void setAndGetPropertyNamingStrategy() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.setPropertyNamingStrategy("SNAKE_CASE");
        assertThat(props.getPropertyNamingStrategy()).isEqualTo("SNAKE_CASE");
    }

    @Test
    void visibilityMap() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.getVisibility().put(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        assertThat(props.getVisibility()).containsEntry(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    @Test
    void serializationMap() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.getSerialization().put(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        assertThat(props.getSerialization()).containsEntry(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Test
    void deserializationMap() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.getDeserialization().put(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        assertThat(props.getDeserialization()).containsEntry(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    void mapperMap() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.getMapper().put(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        assertThat(props.getMapper()).containsEntry(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    }

    @Test
    void defaultPropertyInclusion() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        assertThat(props.getDefaultPropertyInclusion()).isEqualTo(JsonInclude.Include.NON_NULL);
    }

    @Test
    void timeZone() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        TimeZone tz = TimeZone.getTimeZone("America/Los_Angeles");
        props.setTimeZone(tz);
        assertThat(props.getTimeZone()).isEqualTo(tz);
    }

    @Test
    void locale() {
        RedisJacksonProperties props = new RedisJacksonProperties();
        props.setLocale(Locale.US);
        assertThat(props.getLocale()).isEqualTo(Locale.US);
    }
}
