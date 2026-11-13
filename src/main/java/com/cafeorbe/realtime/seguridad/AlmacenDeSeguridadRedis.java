package com.cafeorbe.realtime.seguridad;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "redis", matchIfMissing = true)
public class AlmacenDeSeguridadRedis implements AlmacenDeSeguridad {

    /** Si una réplica muere sin restar sus conexiones, el contador no queda inflado para siempre. */
    private static final Duration VIDA_CONTADOR_CONEXIONES = Duration.ofHours(1);

    /**
     * SEG-04: lee y borra el ticket en una sola operación atómica. Es lo mismo que GETDEL, pero GETDEL solo existe
     * desde Redis 6.2 y Azure Cache for Redis (Basic, Standard y Premium) llega a la 6.0: un script Lua funciona en
     * cualquier versión y Redis lo ejecuta sin intercalar otros comandos, así que dos réplicas no pueden consumir
     * el mismo ticket.
     */
    static final RedisScript<String> CONSUMIR_TICKET = RedisScript.of(
            "local v = redis.call('GET', KEYS[1]) if v then redis.call('DEL', KEYS[1]) end return v", String.class);

    private final StringRedisTemplate redis;

    public AlmacenDeSeguridadRedis(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public Optional<String> consumirTicket(String ticket) {
        return Optional.ofNullable(redis.execute(CONSUMIR_TICKET, List.of(PREFIJO_TICKET + ticket)));
    }

    @Override
    public long contar(String clave, Duration ventana) {
        Long total = redis.opsForValue().increment(clave);
        if (total != null && total == 1L) {
            redis.expire(clave, ventana);
        }
        return total == null ? 0 : total;
    }

    @Override
    public void bloquear(UUID usuario, Duration duracion) {
        redis.opsForValue().set(PREFIJO_BLOQUEO + usuario, "1", duracion);
    }

    @Override
    public boolean estaBloqueado(UUID usuario) {
        return Boolean.TRUE.equals(redis.hasKey(PREFIJO_BLOQUEO + usuario));
    }

    @Override
    public long sumarConexion(UUID usuario) {
        String clave = PREFIJO_CONEXIONES + usuario;
        Long total = redis.opsForValue().increment(clave);
        redis.expire(clave, VIDA_CONTADOR_CONEXIONES);
        return total == null ? 0 : total;
    }

    @Override
    public void restarConexion(UUID usuario) {
        String clave = PREFIJO_CONEXIONES + usuario;
        Long total = redis.opsForValue().decrement(clave);
        if (total != null && total <= 0) {
            redis.delete(clave);
        }
    }

    @Override
    public Set<String> cerradas(Collection<String> jtis) {
        List<String> lista = new ArrayList<>(jtis);
        return presentes(lista, lista.stream().map(j -> PREFIJO_CERRADA + j).toList());
    }

    @Override
    public Set<UUID> bloqueados(Collection<UUID> usuarios) {
        List<UUID> lista = new ArrayList<>(usuarios);
        return presentes(lista, lista.stream().map(u -> PREFIJO_BLOQUEO + u).toList());
    }

    /** Una sola ida a Redis (MGET) para todas las claves. */
    private <T> Set<T> presentes(List<T> elementos, List<String> claves) {
        Set<T> resultado = new HashSet<>();
        if (claves.isEmpty()) {
            return resultado;
        }
        List<String> valores = redis.opsForValue().multiGet(claves);
        if (valores == null) {
            return resultado;
        }
        for (int i = 0; i < valores.size(); i++) {
            if (valores.get(i) != null) {
                resultado.add(elementos.get(i));
            }
        }
        return resultado;
    }
}
