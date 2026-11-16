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
}
