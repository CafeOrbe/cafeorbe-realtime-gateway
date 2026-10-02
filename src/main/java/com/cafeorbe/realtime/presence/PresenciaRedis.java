package com.cafeorbe.realtime.presence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Presencia en Redis: un hash por sala con {@code instancia:sesionId → usuarioId}. Contar valores distintos evita
 * duplicar al mismo usuario si abre la sala en dos pestañas. El hash expira solo si nadie lo toca en 6 horas.
 *
 * <p>Cada instancia renueva un latido con vida corta. Si una instancia se reinicia o se cae, sus conexiones
 * mueren sin pasar por {@link #liberar}: al contar se descartan las que pertenecen a una instancia sin latido,
 * para que no queden como conectados fantasma.
 */
@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "redis", matchIfMissing = true)
public class PresenciaRedis implements Presencia, SmartLifecycle {

    private static final Duration VIDA = Duration.ofHours(6);
    private static final Duration VIDA_DEL_LATIDO = Duration.ofSeconds(30);
    private static final long INTERVALO_DEL_LATIDO_MS = 10_000;

    private final StringRedisTemplate redis;
    private final String instancia;
    private volatile boolean apagada;

    @Autowired
    public PresenciaRedis(StringRedisTemplate redis) {
        this(redis, UUID.randomUUID().toString());
    }

    PresenciaRedis(StringRedisTemplate redis, String instancia) {
        this.redis = redis;
        this.instancia = instancia;
    }

    /** Avisa que esta instancia sigue viva. Se renueva mucho antes de que venza. */
    @Scheduled(fixedDelay = INTERVALO_DEL_LATIDO_MS)
    public void latir() {
        if (!apagada) {
            redis.opsForValue().set(claveDeInstancia(instancia), "1", VIDA_DEL_LATIDO);
        }
    }

    @Override
    public void start() {
        apagada = false;
    }

    /** Apagado ordenado (reinicio o despliegue): sus conexiones dejan de contar de inmediato, sin esperar al latido. */
    @Override
    public void stop() {
        apagada = true;
        redis.delete(claveDeInstancia(instancia));
    }

    @Override
    public boolean isRunning() {
        return !apagada;
    }

    /** Se detiene antes que la conexión a Redis (fase 0), que todavía hace falta para retirar el latido. */
    @Override
    public int getPhase() {
        return 1;
    }

    @Override
    public int registrar(UUID subastaId, String sesionId, UUID usuarioId) {
        String clave = clave(subastaId);
        HashOperations<String, String, String> hash = redis.opsForHash();
        hash.put(clave, campo(sesionId), usuarioId.toString());
        redis.expire(clave, VIDA);
        return contar(clave);
    }

    @Override
    public int liberar(UUID subastaId, String sesionId) {
        String clave = clave(subastaId);
        HashOperations<String, String, String> hash = redis.opsForHash();
        hash.delete(clave, campo(sesionId));
        return contar(clave);
    }

    /** Usuarios distintos con una conexión viva; de paso borra las conexiones de instancias que ya no laten. */
    private int contar(String clave) {
        HashOperations<String, String, String> hash = redis.opsForHash();
        Map<String, Boolean> vivas = new HashMap<>();
        Set<String> usuarios = new HashSet<>();
        hash.entries(clave).forEach((campo, usuarioId) -> {
            int separador = campo.indexOf(':');
            // Un campo sin instancia es del formato anterior: su instancia ya no existe.
            String duena = separador < 0 ? "" : campo.substring(0, separador);
            boolean viva = vivas.computeIfAbsent(duena,
                    i -> i.equals(instancia) || (!i.isEmpty() && Boolean.TRUE.equals(redis.hasKey(claveDeInstancia(i)))));
            if (viva) {
                usuarios.add(usuarioId);
            } else {
                hash.delete(clave, campo);
            }
        });
        return usuarios.size();
    }

    private String campo(String sesionId) {
        return instancia + ":" + sesionId;
    }

    private static String clave(UUID subastaId) {
        return "presencia:sala:" + subastaId;
    }

    private static String claveDeInstancia(String instancia) {
        return "presencia:instancia:" + instancia;
    }
}
