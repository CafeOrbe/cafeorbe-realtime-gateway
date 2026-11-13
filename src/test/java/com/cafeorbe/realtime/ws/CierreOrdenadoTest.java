package com.cafeorbe.realtime.ws;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CierreOrdenadoTest {

    @Test
    @DisplayName("DIS-01 · Al apagar la réplica cierra cada WebSocket con 1012 para que el navegador reconecte en otra")
    void cierraLasConexionesAlApagarse() throws IOException {
        var salas = mock(Salas.class);
        var ana = mock(WebSocketSession.class);
        var bea = mock(WebSocketSession.class);
        // Un cierre que falla no impide cerrar las demás.
        doThrow(new IOException("ya cerrada")).when(ana).close(CloseStatus.SERVICE_RESTARTED);
        when(salas.todas()).thenReturn(List.of(new Salas.Conexion(ana, null), new Salas.Conexion(bea, null)));
        var cierre = new CierreOrdenado(salas);

        cierre.start();
        assertThat(cierre.isRunning()).isTrue();
        cierre.stop();

        assertThat(cierre.isRunning()).isFalse();
        verify(ana).close(CloseStatus.SERVICE_RESTARTED);
        verify(bea).close(CloseStatus.SERVICE_RESTARTED);
    }

    @Test
    @DisplayName("DIS-01 · Se detiene antes que el servidor web, para que los cierres alcancen a salir")
    void seDetieneAntesQueElServidorWeb() {
        // Sin conexiones abiertas (el mock devuelve una lista vacía) el apagado no hace nada más.
        var cierre = new CierreOrdenado(mock(Salas.class));

        assertThat(cierre.getPhase()).isEqualTo(Integer.MAX_VALUE);
        cierre.stop();
        assertThat(cierre.isRunning()).isFalse();
    }
}
