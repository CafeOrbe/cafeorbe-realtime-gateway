package com.cafeorbe.realtime.seguridad;

import com.cafeorbe.contracts.Rol;
import com.cafeorbe.realtime.ws.Identidad;
import com.cafeorbe.realtime.ws.Salas;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SeguridadRealtimeTest {

    private final SimpleMeterRegistry metricas = new SimpleMeterRegistry();

    @Test
    @DisplayName("SEG-04 · En Redis el ticket se consume con un script atómico: el segundo intento no lo encuentra")
    void ticketRedis() {
        var redis = mock(StringRedisTemplate.class);
        when(redis.execute(AlmacenDeSeguridadRedis.CONSUMIR_TICKET, List.of("cafeorbe:ws:ticket:t1")))
                .thenReturn("{}", (String) null);
        var almacen = new AlmacenDeSeguridadRedis(redis);

        assertThat(almacen.consumirTicket("t1")).contains("{}");
        assertThat(almacen.consumirTicket("t1")).isEmpty();
    }

    @Test
    @DisplayName("SEG-04 · El script no usa GETDEL (Redis 6.2+): funciona en Azure Cache for Redis 6.0")
    void scriptCompatibleConRedis6() {
        String script = AlmacenDeSeguridadRedis.CONSUMIR_TICKET.getScriptAsString();

        assertThat(script).doesNotContainIgnoringCase("GETDEL").contains("'GET'").contains("'DEL'");
        assertThat(AlmacenDeSeguridadRedis.CONSUMIR_TICKET.getResultType()).isEqualTo(String.class);
    }

    @Test
    @DisplayName("SEG-03/05 · Contadores, bloqueos, conexiones y consultas en lote (MGET) sobre Redis")
    @SuppressWarnings("unchecked")
    void operacionesRedis() {
        var redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valores = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(valores);
        UUID ana = UUID.randomUUID();
        UUID luis = UUID.randomUUID();
        when(valores.increment("k")).thenReturn(1L, 2L);
        when(valores.increment("cafeorbe:ws:conexiones:" + ana)).thenReturn(3L);
        when(valores.decrement("cafeorbe:ws:conexiones:" + ana)).thenReturn(0L);
        when(redis.hasKey("cafeorbe:seg:bloqueado:" + ana)).thenReturn(true);
        when(valores.multiGet(List.of("cafeorbe:sesion:cerrada:j1", "cafeorbe:sesion:cerrada:j2")))
                .thenReturn(Arrays.asList("1", null));
        when(valores.multiGet(List.of("cafeorbe:seg:bloqueado:" + ana, "cafeorbe:seg:bloqueado:" + luis)))
                .thenReturn(Arrays.asList(null, "1"));
        var almacen = new AlmacenDeSeguridadRedis(redis);

        assertThat(almacen.contar("k", Duration.ofSeconds(2))).isEqualTo(1);
        assertThat(almacen.contar("k", Duration.ofSeconds(2))).isEqualTo(2);
        verify(redis).expire("k", Duration.ofSeconds(2));
        assertThat(almacen.sumarConexion(ana)).isEqualTo(3);
        almacen.restarConexion(ana);
        verify(redis).delete("cafeorbe:ws:conexiones:" + ana);
        almacen.bloquear(ana, Duration.ofSeconds(60));
        verify(valores).set("cafeorbe:seg:bloqueado:" + ana, "1", Duration.ofSeconds(60));
        assertThat(almacen.estaBloqueado(ana)).isTrue();
        assertThat(almacen.cerradas(List.of("j1", "j2"))).containsExactly("j1");
        assertThat(almacen.bloqueados(List.of(ana, luis))).containsExactly(luis);
        assertThat(almacen.cerradas(List.of())).isEmpty();
    }

    @Test
    @DisplayName("SEG-03 · 3 infracciones en un minuto bloquean al usuario una sola vez y suben la métrica")
    void bloqueoTrasTresInfracciones() {
        var almacen = new AlmacenDeSeguridadEnMemoria();
        var abuso = new ControlDeAbuso(almacen, new Auditoria(metricas), metricas, Clock.systemUTC(), 10, 5, 1024, 3,
                Duration.ofSeconds(60));
        UUID ana = UUID.randomUUID();

        assertThat(abuso.infraccion(ana, ControlDeAbuso.MOTIVO_PUJAS, "1.2.3.4", "c")).isFalse();
        assertThat(abuso.infraccion(ana, ControlDeAbuso.MOTIVO_PUJAS, "1.2.3.4", "c")).isFalse();
        assertThat(abuso.infraccion(ana, ControlDeAbuso.MOTIVO_PUJAS, "1.2.3.4", "c")).isTrue();
        assertThat(abuso.infraccion(ana, ControlDeAbuso.MOTIVO_PUJAS, "1.2.3.4", "c")).isFalse();
        assertThat(abuso.estaBloqueado(ana)).isTrue();
        assertThat(metricas.counter("cafeorbe.seguridad.bloqueos").count()).isEqualTo(1);
        assertThat(metricas.counter("cafeorbe.seguridad.infracciones", "motivo", ControlDeAbuso.MOTIVO_PUJAS).count())
                .isEqualTo(4);
    }

    @Test
    @DisplayName("SEG-03 · Ventana de un segundo por usuario: la puja 11 del mismo segundo no pasa")
    void limiteDePujas() {
        var reloj = Clock.fixed(Instant.parse("2026-11-10T10:00:00Z"), ZoneOffset.UTC);
        var abuso = new ControlDeAbuso(new AlmacenDeSeguridadEnMemoria(), new Auditoria(metricas), metricas, reloj,
                10, 5, 1024, 3, Duration.ofSeconds(60));
        UUID ana = UUID.randomUUID();
        for (int i = 0; i < 10; i++) {
            assertThat(abuso.pujaPermitida(ana)).isTrue();
        }
        assertThat(abuso.pujaPermitida(ana)).isFalse();
        assertThat(abuso.pujaPermitida(UUID.randomUUID())).isTrue();
        assertThat(abuso.mensajeDemasiadoGrande(1025)).isTrue();
        assertThat(abuso.mensajeDemasiadoGrande(1024)).isFalse();
    }

    @Test
    @DisplayName("SEG-05 · Sin Redis el vigilante igual corta las sesiones vencidas (reloj local)")
    void vigilanteSinRedis() throws Exception {
        var salas = mock(Salas.class);
        var almacen = mock(AlmacenDeSeguridad.class);
        var vencida = mock(WebSocketSession.class);
        var vigente = mock(WebSocketSession.class);
        var ahora = Instant.parse("2026-11-10T10:00:00Z");
        when(salas.todas()).thenReturn(List.of(
                new Salas.Conexion(vencida, new Identidad(UUID.randomUUID(), "A", Rol.COMPRADOR, "j1", ahora)),
                new Salas.Conexion(vigente, new Identidad(UUID.randomUUID(), "B", Rol.COMPRADOR, "j2",
                        ahora.plusSeconds(60)))));
        when(almacen.cerradas(anyList())).thenThrow(new IllegalStateException("Redis caído"));
        doThrow(new IllegalStateException("ya cerrada")).when(vigente).close(any(CloseStatus.class));

        new VigilanteDeSesiones(salas, almacen, new Auditoria(metricas), Clock.fixed(ahora, ZoneOffset.UTC)).revisar();

        verify(vencida).close(new CloseStatus(4401, "Sesión cerrada o vencida"));
        verify(vigente, never()).close(any(CloseStatus.class));
    }

    @Test
    @DisplayName("Sin conexiones abiertas el vigilante no consulta Redis")
    void vigilanteSinConexiones() {
        var salas = mock(Salas.class);
        var almacen = mock(AlmacenDeSeguridad.class);
        when(salas.todas()).thenReturn(List.of());
        new VigilanteDeSesiones(salas, almacen, new Auditoria(metricas)).revisar();
        verify(almacen, never()).cerradas(any());
        assertThat(new AlmacenDeSeguridadEnMemoria().bloqueados(Set.of(UUID.randomUUID()))).isEmpty();
    }
}
