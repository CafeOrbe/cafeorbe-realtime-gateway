package com.cafeorbe.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REN-01: Coordinador de pujas sobre Redis compartido entre réplicas de realtime-gateway.
 */
@Component
@Primary
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "redis", matchIfMissing = true)
public class CoordinadorDePujasRedis implements CoordinadorDePujas {

    private static final Logger log = LoggerFactory.getLogger(CoordinadorDePujasRedis.class);
    private static final Duration TTL_VUELO = Duration.ofSeconds(3);
    private static final Duration TTL_MINIMO = Duration.ofHours(24);

    private final StringRedisTemplate redis;
    private final ConcurrentHashMap<UUID, Long> minimosLocales = new ConcurrentHashMap<>();

    public CoordinadorDePujasRedis(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public boolean intentarAdquirirVuelo(UUID subastaId, long monto, UUID usuarioId) {
        try {
            String clave = "cafeorbe:subasta:" + subastaId + ":vuelo:" + monto;
            Boolean ok = redis.opsForValue().setIfAbsent(clave, usuarioId.toString(), TTL_VUELO);
            return Boolean.TRUE.equals(ok);
        } catch (Exception e) {
            log.warn("Fallo al acceder a Redis para coordinar vuelo: {}", e.getMessage());
            return true;
        }
    }

    @Override
    public void liberarVuelo(UUID subastaId, long monto) {
        try {
            redis.delete("cafeorbe:subasta:" + subastaId + ":vuelo:" + monto);
        } catch (Exception e) {
            log.debug("No se pudo liberar vuelo de puja: {}", e.getMessage());
        }
    }

    @Override
    public boolean cumpleMinimo(UUID subastaId, long monto) {
        Long local = minimosLocales.get(subastaId);
        if (local != null && monto < local) {
            return false;
        }
        try {
            String str = redis.opsForValue().get("cafeorbe:subasta:" + subastaId + ":minimo");
            if (str != null) {
                long min = Long.parseLong(str);
                minimosLocales.compute(subastaId, (k, v) -> v == null ? min : Math.max(v, min));
                return monto >= min;
            }
        } catch (Exception e) {
            log.warn("Fallo al leer mínimo de Redis: {}", e.getMessage());
        }
        return true;
    }

    @Override
    public Optional<Long> siguienteMinimo(UUID subastaId) {
        Long local = minimosLocales.get(subastaId);
        if (local != null) {
            return Optional.of(local);
        }
        try {
            String str = redis.opsForValue().get("cafeorbe:subasta:" + subastaId + ":minimo");
            if (str != null) {
                long min = Long.parseLong(str);
                minimosLocales.put(subastaId, min);
                return Optional.of(min);
            }
        } catch (Exception ignored) {
        }
        return Optional.empty();
    }

    @Override
    public void registrarMinimo(UUID subastaId, long nuevoMinimo) {
        minimosLocales.compute(subastaId, (k, v) -> v == null ? nuevoMinimo : Math.max(v, nuevoMinimo));
        try {
            redis.opsForValue().set("cafeorbe:subasta:" + subastaId + ":minimo", String.valueOf(nuevoMinimo), TTL_MINIMO);
        } catch (Exception e) {
            log.warn("Fallo al registrar mínimo en Redis: {}", e.getMessage());
        }
    }
}
