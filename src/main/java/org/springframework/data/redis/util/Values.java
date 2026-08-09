package org.springframework.data.redis.util;

import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
/**
 * Values.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */

public class Values {

    public static boolean nonNull(Object value) {
        if(Objects.isNull(value)){
            return false;
        }
        if(value instanceof Collection){
            return !CollectionUtils.isEmpty((Collection) value);
        }
        if(value instanceof Map){
            return !CollectionUtils.isEmpty((Map) value);
        }
        return Boolean.TRUE;
    }

}
