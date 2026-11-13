package com.cafeorbe.realtime.seguridad;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Registro de auditoría de seguridad del realtime-gateway (SEG-03, SEG-04, SEG-05): una línea JSON por evento
 * con usuario, IP, motivo y correlationId, y la métrica {@code cafeorbe_seguridad_eventos_total}, que es la que
 * lee el tablero técnico. Nunca registra el ticket ni la URL de conexión.
 */
@Component
public class Auditoria {

    private static final Logger log = LoggerFactory.getLogger("cafeorbe.seguridad");

    private final MeterRegistry metricas;

    public Auditoria(MeterRegistry metricas) {
        this.metricas = metricas;
    }

    public void evento(String evento, String motivo, Object usuario, String ip, String correlationId) {
        metricas.counter("cafeorbe.seguridad.eventos", "evento", evento, "motivo", motivo).increment();
        log.atWarn()
                .addKeyValue("evento", evento)
                .addKeyValue("motivo", motivo)
                .addKeyValue("usuario", usuario == null ? "desconocido" : usuario.toString())
                .addKeyValue("ip", ip == null ? "desconocida" : ip)
                .addKeyValue("correlationId", correlationId)
                .log("Evento de seguridad: {} ({})", evento, motivo);
    }
}
