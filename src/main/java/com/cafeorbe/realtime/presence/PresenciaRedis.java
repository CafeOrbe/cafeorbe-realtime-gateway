package com.cafeorbe.realtime.presence;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

/**
 * Presencia en Redis: un hash por sala con {@code sesionId → usuarioId}. Contar valores distintos evita
 * duplicar al mismo usuario si abre la sala en dos pestañas. El hash expira solo si nadie lo toca en 6 horas.
 */
@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "redis", matchIfMissing = true)
public class PresenciaRedis implements Presencia {

    private static final Duration VIDA = Duration.ofHours(6);

    private final StringRedisTemplate redis;

    public PresenciaRedis(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public int registrar(UUID subastaId, String sesionId, UUID usuarioId) {
        String clave = clave(subastaId);
        HashOperations<String, String, String> hash = redis.opsForHash();
        hash.put(clave, sesionId, usuarioId.toString());
        redis.expire(clave, VIDA);
        return contar(clave);
    }

    @Override
    public int liberar(UUID subastaId, String sesionId) {
        String clave = clave(subastaId);
        HashOperations<String, String, String> hash = redis.opsForHash();
        hash.delete(clave, sesionId);
        return contar(clave);
    }

    private int contar(String clave) {
        HashOperations<String, String, String> hash = redis.opsForHash();
        return (int) hash.values(clave).stream().distinct().count();
    }

    private static String clave(UUID subastaId) {
        return "presencia:sala:" + subastaId;
    }
}
