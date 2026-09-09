package redistpl.plus.spring.boot;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisOperationTemplate;
import org.springframework.data.redis.core.RedisOperationTemplate;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisFacadeApiContractTest {
    @Test void shouldKeepSynchronousFacadePublicApi() throws Exception {
        assertApi(RedisOperationTemplate.class, 431, "ed57b51b57979aed518a3aa0ca7e34985101a9a5cc698daeb8c4880d26f4ae36");
    }
    @Test void shouldKeepReactiveFacadePublicApi() throws Exception {
        assertApi(ReactiveRedisOperationTemplate.class, 293, "66c9945fb938bb5c946ee70691929960741fdc3263f688553af461bb66e817cf");
    }
    private static void assertApi(Class<?> type, int count, String hash) throws Exception {
        List<String> signatures = new ArrayList<String>();
        for (Method method : type.getDeclaredMethods()) if (Modifier.isPublic(method.getModifiers())) signatures.add(method.toGenericString());
        Collections.sort(signatures);
        assertEquals(count, signatures.size());
        String actualHash = sha256(String.join("\n", signatures));
        assertEquals(hash, actualHash, actualHash);
    }
    private static String sha256(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        StringBuilder text = new StringBuilder(digest.length * 2);
        for (byte item : digest) text.append(String.format("%02x", item & 0xff));
        return text.toString();
    }
}
