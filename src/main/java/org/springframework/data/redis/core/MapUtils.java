package org.springframework.data.redis.core;

import java.util.Map;
/**
 * MapUtils.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */

public class MapUtils {

    /**
     * Gets a String from a Map in a null-safe manner.
     * <p>
     * The String is obtained via <code>toString</code>.
     *
     * @param map  the map to use
     * @param key  the key to look up
     * @return the value in the Map as a String, <code>null</code> if null map input
     */
    public static String getString(final Map map, final Object key) {
        if (map != null) {
            Object answer = map.get(key);
            if (answer != null) {
                return answer.toString();
            }
        }
        return null;
    }



}
