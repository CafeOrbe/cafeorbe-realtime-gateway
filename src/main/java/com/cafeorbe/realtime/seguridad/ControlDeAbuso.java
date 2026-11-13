package com.cafeorbe.realtime.seguridad;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

/**
 * SEG-03: límites por usuario contra ráfagas abusivas.
 * <ul>
 *   <li>Máximo {@code pujasPorSegundo} mensajes PUJAR por segundo (el excedente no llega a auction).</li>
 *   <li>Máximo {@code conexionesPorUsuario} conexiones WebSocket abiertas a la vez, entre todas las réplicas.</li>
 *   <li>Mensajes de más de {@code bytesPorMensaje} se cortan con cierre 1009.</li>
 *   <li>{@code infraccionesParaBloquear} infracciones en un minuto bloquean al usuario durante {@code bloqueo}.</li>
 * </ul>
 */
@Component
public class ControlDeAbuso {

    public static final String MOTIVO_PUJAS = "limite_pujas_por_segundo";
    public static final String MOTIVO_CONEXIONES = "limite_conexiones";
    public static final String MOTIVO_TAMANO = "mensaje_demasiado_grande";

    private static final Duration VENTANA_INFRACCIONES = Duration.ofMinutes(1);

    private final AlmacenDeSeguridad almacen;
    private final Auditoria auditoria;
    private final MeterRegistry metricas;
    private final Clock reloj;
    private final int pujasPorSegundo;
    private final int conexionesPorUsuario;
    private final int bytesPorMensaje;
    private final int infraccionesParaBloquear;
    private final Duration bloqueo;

    @Autowired
    public ControlDeAbuso(AlmacenDeSeguridad almacen, Auditoria auditoria, MeterRegistry metricas,
                          @Value("${cafeorbe.abuso.pujas-por-segundo:10}") int pujasPorSegundo,
                          @Value("${cafeorbe.abuso.conexiones-por-usuario:5}") int conexionesPorUsuario,
                          @Value("${cafeorbe.abuso.bytes-por-mensaje:1024}") int bytesPorMensaje,
                          @Value("${cafeorbe.abuso.infracciones-para-bloquear:3}") int infraccionesParaBloquear,
                          @Value("${cafeorbe.abuso.segundos-de-bloqueo:60}") long segundosDeBloqueo) {
        this(almacen, auditoria, metricas, Clock.systemUTC(), pujasPorSegundo, conexionesPorUsuario, bytesPorMensaje,
                infraccionesParaBloquear, Duration.ofSeconds(segundosDeBloqueo));
    }

    ControlDeAbuso(AlmacenDeSeguridad almacen, Auditoria auditoria, MeterRegistry metricas, Clock reloj,
                   int pujasPorSegundo, int conexionesPorUsuario, int bytesPorMensaje, int infraccionesParaBloquear,
                   Duration bloqueo) {
        this.almacen = almacen;
        this.auditoria = auditoria;
        this.metricas = metricas;
        this.reloj = reloj;
        this.pujasPorSegundo = pujasPorSegundo;
        this.conexionesPorUsuario = conexionesPorUsuario;
        this.bytesPorMensaje = bytesPorMensaje;
        this.infraccionesParaBloquear = infraccionesParaBloquear;
        this.bloqueo = bloqueo;
    }

    /** Ventana fija de un segundo, compartida entre réplicas. */
    public boolean pujaPermitida(UUID usuario) {
        long segundo = reloj.millis() / 1000;
        return almacen.contar("cafeorbe:rl:puja:" + usuario + ":" + segundo, Duration.ofSeconds(2)) <= pujasPorSegundo;
    }

    public boolean mensajeDemasiadoGrande(int bytes) {
        return bytes > bytesPorMensaje;
    }

    /** @return {@code false} si con esta conexión el usuario supera su máximo (y en ese caso no queda contada) */
    public boolean abrirConexion(UUID usuario) {
        if (almacen.sumarConexion(usuario) > conexionesPorUsuario) {
            almacen.restarConexion(usuario);
            return false;
        }
        return true;
    }

    public void cerrarConexion(UUID usuario) {
        almacen.restarConexion(usuario);
    }

    public boolean estaBloqueado(UUID usuario) {
        return almacen.estaBloqueado(usuario);
    }

    /**
     * Anota una infracción. Al llegar a {@code infraccionesParaBloquear} en un minuto bloquea al usuario,
     * registra el evento de seguridad y sube la métrica de bloqueos.
     *
     * @return {@code true} si con esta infracción el usuario quedó bloqueado
     */
    public boolean infraccion(UUID usuario, String motivo, String ip, String correlationId) {
        metricas.counter("cafeorbe.seguridad.infracciones", "motivo", motivo).increment();
        auditoria.evento("infraccion", motivo, usuario, ip, correlationId);
        long total = almacen.contar("cafeorbe:seg:infracciones:" + usuario, VENTANA_INFRACCIONES);
        if (total >= infraccionesParaBloquear && !almacen.estaBloqueado(usuario)) {
            almacen.bloquear(usuario, bloqueo);
            metricas.counter("cafeorbe.seguridad.bloqueos").increment();
            auditoria.evento("usuario_bloqueado", motivo, usuario, ip, correlationId);
            return true;
        }
        return false;
    }
}
