package com.cafeorbe.realtime.presence;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Presencia compartida entre instancias, con un Redis simulado en memoria. */
class PresenciaRedisTest {

    static final UUID SALA = UUID.randomUUID();
    static final UUID ANA = UUID.randomUUID();
    static final UUID BRUNO = UUID.randomUUID();

    private final Map<String, Map<String, String>> hashes = new HashMap<>();
    private final Set<String> claves = new HashSet<>();
    private StringRedisTemplate redis;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void redisSimulado() {
        redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hash = mock(HashOperations.class);
        ValueOperations<String, String> valores = mock(ValueOperations.class);
        doReturn(hash).when(redis).opsForHash();
        when(redis.opsForValue()).thenReturn(valores);
        when(redis.hasKey(anyString())).thenAnswer(i -> claves.contains(i.<String>getArgument(0)));
        when(redis.delete(anyString())).thenAnswer(i -> claves.remove(i.<String>getArgument(0)));

        doAnswer(i -> hashes.computeIfAbsent(i.getArgument(0), k -> new HashMap<>()).put(i.getArgument(1), i.getArgument(2)))
                .when(hash).put(anyString(), any(), any());
        when(hash.delete(anyString(), any())).thenAnswer(i ->
                hashes.getOrDefault(i.<String>getArgument(0), new HashMap<>()).remove(i.<String>getArgument(1)) == null ? 0L : 1L);
        when(hash.entries(anyString())).thenAnswer(i -> new HashMap<>(hashes.getOrDefault(i.<String>getArgument(0), Map.of())));
        doAnswer(i -> claves.add(i.getArgument(0))).when(valores).set(anyString(), anyString(), any(Duration.class));
    }

    private PresenciaRedis instancia(String nombre) {
        PresenciaRedis presencia = new PresenciaRedis(redis, nombre);
        presencia.latir();
        return presencia;
    }

    @Test
    @DisplayName("HU-05 · Registra al entrar, libera al salir y no duplica al mismo usuario en dos pestañas")
    void registraYLibera() {
        PresenciaRedis a = instancia("a");

        assertThat(a.registrar(SALA, "s1", ANA)).isEqualTo(1);
        assertThat(a.registrar(SALA, "s2", ANA)).isEqualTo(1);
        assertThat(a.registrar(SALA, "s3", BRUNO)).isEqualTo(2);
        assertThat(a.liberar(SALA, "s3")).isEqualTo(1);
        assertThat(a.liberar(SALA, "s1")).isEqualTo(1);
        assertThat(a.liberar(SALA, "s2")).isZero();
    }

    @Test
    @DisplayName("HU-05 · Varias instancias vivas comparten el conteo de la sala")
    void variasInstancias() {
        PresenciaRedis a = instancia("a");
        PresenciaRedis b = instancia("b");

        a.registrar(SALA, "s1", ANA);
        assertThat(b.registrar(SALA, "s1", BRUNO)).isEqualTo(2);
    }

    @Test
    @DisplayName("HU-05 · Las conexiones de una instancia caída no quedan como conectados fantasma")
    void instanciaCaida() {
        PresenciaRedis a = instancia("a");
        a.registrar(SALA, "s1", ANA);

        // La instancia "a" se cae sin liberar sus conexiones y su latido vence.
        claves.remove("presencia:instancia:a");
        PresenciaRedis b = instancia("b");

        assertThat(b.registrar(SALA, "s1", BRUNO)).isEqualTo(1);
        assertThat(hashes.get("presencia:sala:" + SALA)).containsOnlyKeys("b:s1");
    }

    @Test
    @DisplayName("HU-05 · Una instancia que se apaga de forma ordenada deja de contar de inmediato")
    void apagadoOrdenado() {
        PresenciaRedis a = instancia("a");
        a.registrar(SALA, "s1", ANA);
        a.stop();
        a.latir();

        assertThat(instancia("b").registrar(SALA, "s1", BRUNO)).isEqualTo(1);
    }

    @Test
    @DisplayName("Un registro del formato anterior (sin instancia) se descarta")
    void formatoAnterior() {
        hashes.computeIfAbsent("presencia:sala:" + SALA, k -> new HashMap<>()).put("sesion-vieja", ANA.toString());

        assertThat(instancia("b").registrar(SALA, "s1", BRUNO)).isEqualTo(1);
    }
}
