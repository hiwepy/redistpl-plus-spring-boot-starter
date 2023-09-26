package org.springframework.data.redis.util;

import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        return true;
    }

}
