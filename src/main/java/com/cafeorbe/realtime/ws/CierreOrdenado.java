package com.cafeorbe.realtime.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

import java.io.IOException;

/**
 * DIS-01: cuando una réplica se apaga (escala hacia abajo, despliegue o reemplazo por falla de salud), cierra sus
 * WebSockets con 1012 ("el servicio se reinicia") apenas empieza el apagado. El navegador reconecta en ~1 s con un
 * ticket nuevo, el balanceador lo lleva a una réplica sana y la sala se resincroniza por REST. Sin esto, las
 * conexiones seguían en la réplica que se apaga hasta que terminaba el apagado ordenado (hasta 30 s), sin recibir
 * pujas nuevas en algunos casos.
 *
 * <p>Se detiene antes que el servidor web (fase más alta), así los cierres alcanzan a salir por la red.
 */
@Component
public class CierreOrdenado implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(CierreOrdenado.class);

    private final Salas salas;
    private volatile boolean corriendo;

    public CierreOrdenado(Salas salas) {
        this.salas = salas;
    }

    @Override
    public void start() {
        corriendo = true;
    }

    @Override
    public void stop() {
        corriendo = false;
        var conexiones = salas.todas();
        if (!conexiones.isEmpty()) {
            log.info("Apagado de la réplica: se cierran {} conexiones para que reconecten en otra", conexiones.size());
        }
        for (Salas.Conexion conexion : conexiones) {
            try {
                conexion.sesion().close(CloseStatus.SERVICE_RESTARTED);
            } catch (IOException | RuntimeException e) {
                log.debug("No se pudo cerrar la sesión {}: {}", conexion.sesion().getId(), e.getMessage());
            }
        }
    }

    @Override
    public boolean isRunning() {
        return corriendo;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
