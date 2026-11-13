package com.cafeorbe.realtime.seguridad;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Registro de auditoría de seguridad del realtime-gateway (SEG-03, SEG-04, SEG-05): una línea JSON por evento
 * con usuario, IP, motivo y correlationId, y la métrica {@code cafeorbe_seguridad_eventos_total}, que es la que
 * lee el tablero técnico. Nunca registra el ticket ni la URL de conexión.
 */
@Component
public class Auditoria {

    private static final Logger log = LoggerFactory.getLogger("cafeorbe.seguridad");
    private static final String CLAVE_CORRELACION = "correlationId";

    private final MeterRegistry metricas;

    public Auditoria(MeterRegistry metricas) {
        this.metricas = metricas;
    }

    public void evento(String evento, String motivo, Object usuario, String ip, String correlationId) {
        metricas.counter("cafeorbe.seguridad.eventos", "evento", evento, "motivo", motivo).increment();
        var registro = log.atWarn()
                .addKeyValue("evento", evento)
                .addKeyValue("motivo", motivo)
                .addKeyValue("usuario", usuario == null ? "desconocido" : usuario.toString())
                .addKeyValue("ip", ip == null ? "desconocida" : ip);
        // Al procesar un mensaje el correlationId ya está en el MDC y sale en el JSON. Agregarlo otra vez repite el
        // campo, el escritor JSON falla y el evento se pierde sin aviso: solo se agrega cuando el MDC no lo trae
        // (handshake, vigilante de sesiones).
        if (MDC.get(CLAVE_CORRELACION) == null) {
            registro = registro.addKeyValue(CLAVE_CORRELACION, correlationId);
        }
        registro.log("Evento de seguridad: {} ({})", evento, motivo);
    }
}
