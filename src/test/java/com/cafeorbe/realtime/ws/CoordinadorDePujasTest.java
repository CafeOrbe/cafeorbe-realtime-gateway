package com.cafeorbe.realtime.ws;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoordinadorDePujasTest {

    @Test
    @DisplayName("CoordinadorDePujasEnMemoria: restringe vuelo simultáneo y verifica mínimo")
    void coordinadorEnMemoria() {
        var coordinador = new CoordinadorDePujasEnMemoria();
        UUID subasta = UUID.randomUUID();
        UUID user1 = UUID.randomUUID();
        UUID user2 = UUID.randomUUID();

        // Inicialmente no conoce mínimo: cumple
        assertThat(coordinador.cumpleMinimo(subasta, 1000)).isTrue();
        assertThat(coordinador.siguienteMinimo(subasta)).isEmpty();

        // Primer vuelo adquirido
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 1000, user1)).isTrue();
        // Segundo vuelo simultáneo por el mismo monto debe ser denegado
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 1000, user2)).isFalse();

        // Liberar vuelo permite reintentar
        coordinador.liberarVuelo(subasta, 1000);
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 1000, user2)).isTrue();

        // Registrar nuevo mínimo
        coordinador.registrarMinimo(subasta, 1500);
        assertThat(coordinador.siguienteMinimo(subasta)).contains(1500L);
        assertThat(coordinador.cumpleMinimo(subasta, 1200)).isFalse();
        assertThat(coordinador.cumpleMinimo(subasta, 1500)).isTrue();
        assertThat(coordinador.cumpleMinimo(subasta, 1600)).isTrue();
    }

    @Test
    @DisplayName("CoordinadorDePujasRedis: utiliza operaciones atómicas en Redis con TTL")
    void coordinadorRedis() {
        var redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);

        var coordinador = new CoordinadorDePujasRedis(redis);
        UUID subasta = UUID.randomUUID();
        UUID user1 = UUID.randomUUID();

        // Intento de vuelo exitoso
        when(ops.setIfAbsent(eq("cafeorbe:subasta:" + subasta + ":vuelo:1000"), eq(user1.toString()), any(Duration.class)))
                .thenReturn(true);
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 1000, user1)).isTrue();

        // Intento de vuelo fallido
        when(ops.setIfAbsent(eq("cafeorbe:subasta:" + subasta + ":vuelo:1000"), eq(user1.toString()), any(Duration.class)))
                .thenReturn(false);
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 1000, user1)).isFalse();

        // Liberar vuelo
        coordinador.liberarVuelo(subasta, 1000);
        verify(redis).delete("cafeorbe:subasta:" + subasta + ":vuelo:1000");

        // Registrar mínimo
        coordinador.registrarMinimo(subasta, 2000);
        verify(ops).set(eq("cafeorbe:subasta:" + subasta + ":minimo"), eq("2000"), any(Duration.class));
        assertThat(coordinador.siguienteMinimo(subasta)).contains(2000L);
        assertThat(coordinador.cumpleMinimo(subasta, 1999)).isFalse();
        assertThat(coordinador.cumpleMinimo(subasta, 2000)).isTrue();
    }

    @Test
    @DisplayName("CoordinadorDePujasRedis: sin mínimo local lo lee de Redis (otra réplica lo registró)")
    void minimoDesdeRedis() {
        var redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        UUID subasta = UUID.randomUUID();
        when(ops.get("cafeorbe:subasta:" + subasta + ":minimo")).thenReturn("3000");

        var coordinador = new CoordinadorDePujasRedis(redis);
        assertThat(coordinador.cumpleMinimo(subasta, 2999)).isFalse();
        assertThat(coordinador.cumpleMinimo(subasta, 3000)).isTrue();

        var otro = new CoordinadorDePujasRedis(redis);
        assertThat(otro.siguienteMinimo(subasta)).contains(3000L);
        // Ya quedó en memoria: no vuelve a consultar Redis.
        assertThat(otro.siguienteMinimo(subasta)).contains(3000L);
    }

    @Test
    @DisplayName("CoordinadorDePujasRedis: sin dato en Redis no hay mínimo y la puja sigue a auction")
    void sinMinimoEnRedis() {
        var redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        UUID subasta = UUID.randomUUID();

        var coordinador = new CoordinadorDePujasRedis(redis);
        assertThat(coordinador.cumpleMinimo(subasta, 1)).isTrue();
        assertThat(coordinador.siguienteMinimo(subasta)).isEmpty();
        when(ops.setIfAbsent(any(), any(), any(Duration.class))).thenReturn(null);
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 10, UUID.randomUUID())).isFalse();
    }

    @Test
    @DisplayName("CoordinadorDePujasRedis: si Redis falla no bloquea pujas (auction decide) y conserva el mínimo local")
    void redisCaido() {
        var redis = mock(StringRedisTemplate.class);
        when(redis.opsForValue()).thenThrow(new IllegalStateException("redis caído"));
        when(redis.delete(any(String.class))).thenThrow(new IllegalStateException("redis caído"));
        UUID subasta = UUID.randomUUID();

        var coordinador = new CoordinadorDePujasRedis(redis);
        assertThat(coordinador.intentarAdquirirVuelo(subasta, 100, UUID.randomUUID())).isTrue();
        coordinador.liberarVuelo(subasta, 100);
        assertThat(coordinador.cumpleMinimo(subasta, 100)).isTrue();
        assertThat(coordinador.siguienteMinimo(subasta)).isEmpty();

        coordinador.registrarMinimo(subasta, 500);
        assertThat(coordinador.siguienteMinimo(subasta)).contains(500L);
        assertThat(coordinador.cumpleMinimo(subasta, 499)).isFalse();
    }

    @Test
    @DisplayName("El mínimo registrado nunca baja, en memoria y en Redis")
    void minimoNuncaBaja() {
        var coordinador = new CoordinadorDePujasEnMemoria();
        UUID subasta = UUID.randomUUID();
        coordinador.registrarMinimo(subasta, 900);
        coordinador.registrarMinimo(subasta, 800);
        assertThat(coordinador.siguienteMinimo(subasta)).contains(900L);

        var redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        var enRedis = new CoordinadorDePujasRedis(redis);
        enRedis.registrarMinimo(subasta, 900);
        enRedis.registrarMinimo(subasta, 800);
        assertThat(enRedis.siguienteMinimo(subasta)).contains(900L);
    }
}
