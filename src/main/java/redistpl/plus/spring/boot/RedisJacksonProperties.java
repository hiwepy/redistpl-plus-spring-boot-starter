package redistpl.plus.spring.boot;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * Configuration properties to configure Jackson.
 *
 * @author Andy Wilkinson
 * @author Marcel Overdijk
 * @author Johannes Edmeier
 * @since 1.2.0
 */
@ConfigurationProperties(prefix = "spring.redis.jackson")
public class RedisJacksonProperties {

    /**
     * Date format string or a fully-qualified date format class name. For instance,
     * `yyyy-MM-dd HH:mm:ss`.
     */
    private String dateFormat;

    /**
     * One of the constants on Jackson's PropertyNamingStrategy. Can also be a
     * fully-qualified class name of a PropertyNamingStrategy subclass.
     */
    private String propertyNamingStrategy;

    /**
     * Jackson visibility thresholds that can be used to limit which methods (and fields)
     * are auto-detected.
     */
    private final Map<PropertyAccessor, JsonAutoDetect.Visibility> visibility = new EnumMap<>(PropertyAccessor.class);

    /**
     * Jackson on/off features that affect the way Java objects are serialized.
     */
    private final Map<SerializationFeature, Boolean> serialization = new EnumMap<>(SerializationFeature.class);

    /**
     * Jackson on/off features that affect the way Java objects are deserialized.
     */
    private final Map<DeserializationFeature, Boolean> deserialization = new EnumMap<>(DeserializationFeature.class);

    /**
     * Jackson general purpose on/off features.
     */
    private final Map<MapperFeature, Boolean> mapper = new EnumMap<>(MapperFeature.class);

    /**
     * Jackson on/off features for parsers.
     */
    private final Map<JsonParser.Feature, Boolean> parser = new EnumMap<>(JsonParser.Feature.class);

    /**
     * Jackson on/off features for generators.
     */
    private final Map<JsonGenerator.Feature, Boolean> generator = new EnumMap<>(JsonGenerator.Feature.class);

    /**
     * Controls the inclusion of properties during serialization. Configured with one of
     * the values in Jackson's JsonInclude.Include enumeration.
     */
    private JsonInclude.Include defaultPropertyInclusion;

    /**
     * Time zone used when formatting dates. For instance, "America/Los_Angeles" or
     * "GMT+10".
     */
    private TimeZone timeZone = null;

    /**
     * Locale used for formatting.
     */
    private Locale locale;

    /**
     * <p>Returns the date format.</p>
     * @return the get date format
     */
    public String getDateFormat() {
        return this.dateFormat;
    }

    /**
     * <p>Sets the date format.</p>
     * @param dateFormat
     */
    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    /**
     * <p>Returns the property naming strategy.</p>
     * @return the get property naming strategy
     */
    public String getPropertyNamingStrategy() {
        return this.propertyNamingStrategy;
    }

    /**
     * <p>Sets the property naming strategy.</p>
     * @param propertyNamingStrategy
     */
    public void setPropertyNamingStrategy(String propertyNamingStrategy) {
        this.propertyNamingStrategy = propertyNamingStrategy;
    }

    /**
     * <p>Returns the visibility.</p>
     * @return the get visibility
     */
    public Map<PropertyAccessor, JsonAutoDetect.Visibility> getVisibility() {
        return this.visibility;
    }

    /**
     * <p>Returns the serialization.</p>
     * @return the get serialization
     */
    public Map<SerializationFeature, Boolean> getSerialization() {
        return this.serialization;
    }

    /**
     * <p>Returns the deserialization.</p>
     * @return the get deserialization
     */
    public Map<DeserializationFeature, Boolean> getDeserialization() {
        return this.deserialization;
    }

    /**
     * <p>Returns the mapper.</p>
     * @return the get mapper
     */
    public Map<MapperFeature, Boolean> getMapper() {
        return this.mapper;
    }

    /**
     * <p>Returns the parser.</p>
     * @return the get parser
     */
    public Map<JsonParser.Feature, Boolean> getParser() {
        return this.parser;
    }

    /**
     * <p>Returns the generator.</p>
     * @return the get generator
     */
    public Map<JsonGenerator.Feature, Boolean> getGenerator() {
        return this.generator;
    }

    public JsonInclude.Include getDefaultPropertyInclusion() {
        return this.defaultPropertyInclusion;
    }

    /**
     * <p>Sets the default property inclusion.</p>
     * @param defaultPropertyInclusion
     */
    public void setDefaultPropertyInclusion(JsonInclude.Include defaultPropertyInclusion) {
        this.defaultPropertyInclusion = defaultPropertyInclusion;
    }

    /**
     * <p>Returns the time zone.</p>
     * @return the get time zone
     */
    public TimeZone getTimeZone() {
        return this.timeZone;
    }

    /**
     * <p>Sets the time zone.</p>
     * @param timeZone
     */
    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    /**
     * <p>Returns the locale.</p>
     * @return the get locale
     */
    public Locale getLocale() {
        return this.locale;
    }

    /**
     * <p>Sets the locale.</p>
     * @param locale
     */
    public void setLocale(Locale locale) {
        this.locale = locale;
    }

}
