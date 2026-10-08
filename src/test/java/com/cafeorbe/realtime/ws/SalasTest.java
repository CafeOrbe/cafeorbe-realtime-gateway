package com.cafeorbe.realtime.ws;

import com.cafeorbe.contracts.Rol;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Salas reparte cada sobre entre las conexiones locales. Que una conexión esté caída no puede tumbar al
 * resto de la sala, y el conteo de conectados es lo que ve el usuario en el contador.
 */
class SalasTest {

    static final UUID SALA = UUID.randomUUID();
    static final UUID OTRA = UUID.randomUUID();
    static final UUID ANA = UUID.randomUUID();
    static final UUID BRUNO = UUID.randomUUID();

    private final ObjectMapper json = new ObjectMapper();
    private final Salas salas = new Salas(json);

    private WebSocketSession sesion(boolean abierta, String id) {
        var sesion = mock(WebSocketSession.class);
        when(sesion.isOpen()).thenReturn(abierta);
        when(sesion.getId()).thenReturn(id);
        return sesion;
    }

    private Salas.Conexion conexion(WebSocketSession sesion, UUID usuarioId) {
        return new Salas.Conexion(sesion, new Identidad(usuarioId, "Usuario", Rol.COMPRADOR));
    }

    private Sobre sobre(UUID usuarioId) {
        return new Sobre(SALA, usuarioId, "PUJA_ACEPTADA", json.valueToTree(Map.of("monto", 500)));
    }

    @Test
    @DisplayName("conexionesLocales(): cuenta las conexiones de la sala y 0 en una sala desconocida")
    void conteoDeConexionesLocales() {
        assertThat(salas.conexionesLocales(SALA)).isZero();

        salas.agregar(SALA, conexion(sesion(true, "s1"), ANA));
        assertThat(salas.conexionesLocales(SALA)).isEqualTo(1);

        salas.agregar(SALA, conexion(sesion(true, "s2"), BRUNO));
        assertThat(salas.conexionesLocales(SALA)).isEqualTo(2);
        assertThat(salas.conexionesLocales(OTRA)).isZero();
    }

    @Test
    @DisplayName("quitar(): al salir el último se borra la sala, y el conteo vuelve a 0")
    void quitarDejaLaSalaVacia() {
        salas.agregar(SALA, conexion(sesion(true, "s1"), ANA));
        salas.quitar(SALA, "s1");

        assertThat(salas.conexionesLocales(SALA)).isZero();
        // Quitar de una sala que ya no existe no debe romper nada.
        assertThatCode(() -> salas.quitar(SALA, "s1")).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("entregar(): un sobre sin usuario destino llega a toda la sala")
    void entregaATodaLaSala() throws Exception {
        var ana = sesion(true, "s1");
        var bruno = sesion(true, "s2");
        salas.agregar(SALA, conexion(ana, ANA));
        salas.agregar(SALA, conexion(bruno, BRUNO));

        salas.entregar(new Sobre(SALA, null, "PUJA_ACEPTADA", json.valueToTree(Map.of("monto", 500))));

        var enviado = ArgumentCaptor.forClass(TextMessage.class);
        verify(ana).sendMessage(enviado.capture());
        verify(bruno).sendMessage(enviado.capture());
        assertThat(enviado.getValue().getPayload())
                .contains("\"tipo\":\"PUJA_ACEPTADA\"")
                .contains("\"monto\":500");
    }

    @Test
    @DisplayName("entregar(): un sobre dirigido a un usuario no llega a los demás")
    void entregaSoloAlUsuarioIndicado() throws Exception {
        var ana = sesion(true, "s1");
        var bruno = sesion(true, "s2");
        salas.agregar(SALA, conexion(ana, ANA));
        salas.agregar(SALA, conexion(bruno, BRUNO));

        salas.entregar(sobre(ANA));

        verify(ana).sendMessage(any(TextMessage.class));
        verify(bruno, never()).sendMessage(any(TextMessage.class));
    }

    @Test
    @DisplayName("entregar(): una sesión cerrada no se escribe y no rompe al resto")
    void unaSesionCaidaNoAfectaAlResto() throws Exception {
        var caida = sesion(false, "s1");
        var viva = sesion(true, "s2");
        salas.agregar(SALA, conexion(caida, ANA));
        salas.agregar(SALA, conexion(viva, BRUNO));

        assertThatCode(() -> salas.entregar(sobre(null))).doesNotThrowAnyException();

        verify(caida, never()).sendMessage(any());
        verify(viva).sendMessage(any(TextMessage.class));
    }

    @Test
    @DisplayName("enviar(): una sesión que lanza al escribir no se propaga al que la acompaña")
    void unEnvioQueFallaNoSePropaga() throws IOException {
        var queFalla = sesion(true, "s1");
        doThrow(new IOException("la sesión se cerró de golpe")).when(queFalla).sendMessage(any());
        var sana = sesion(true, "s2");

        assertThatCode(() -> {
            salas.enviar(queFalla, "hola");
            salas.enviar(sana, "hola");
        }).doesNotThrowAnyException();

        verify(sana).sendMessage(any(TextMessage.class));
    }

    @Test
    @DisplayName("entregar(): si el sobre no se puede serializar no se entrega a nadie")
    void sobreNoSerializableNoSeEntrega() throws JsonProcessingException, IOException {
        var jsonRoto = mock(ObjectMapper.class);
        when(jsonRoto.writeValueAsString(any())).thenThrow(new JsonProcessingException("roto") { });
        var salasRotas = new Salas(jsonRoto);
        var ana = sesion(true, "s1");
        salasRotas.agregar(SALA, conexion(ana, ANA));

        assertThatCode(() -> salasRotas.entregar(new Sobre(SALA, null, "X", null)))
                .doesNotThrowAnyException();

        verify(ana, never()).sendMessage(any());
    }

    @Test
    @DisplayName("entregar(): un sobre de una sala que no existe en esta instancia se ignora")
    void sobreDeSalaAjena() throws IOException {
        var ana = sesion(true, "s1");
        salas.agregar(OTRA, conexion(ana, ANA));

        assertThatCode(() -> salas.entregar(new Sobre(SALA, null, "X", null))).doesNotThrowAnyException();

        verify(ana, never()).sendMessage(any());
    }
}
