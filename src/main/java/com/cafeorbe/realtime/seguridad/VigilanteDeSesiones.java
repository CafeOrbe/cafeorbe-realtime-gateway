package com.cafeorbe.realtime.seguridad;

import com.cafeorbe.realtime.ws.Identidad;
import com.cafeorbe.realtime.ws.SalaWebSocketHandler;
import com.cafeorbe.realtime.ws.Salas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * SEG-05 y SEG-03: una vez por segundo revisa las conexiones abiertas en esta réplica y corta las de sesiones
 * vencidas o cerradas (cierre 4401) y las de usuarios bloqueados (cierre 4429). Con una revisión por segundo la
 * conexión se corta en 5 s o menos tras el vencimiento. Una sola consulta a Redis (MGET) por cada tipo de chequeo.
 */
@Component
@ConditionalOnProperty(name = "cafeorbe.vigilante.habilitado", havingValue = "true", matchIfMissing = true)
public class VigilanteDeSesiones {

    private static final Logger log = LoggerFactory.getLogger(VigilanteDeSesiones.class);

    private final Salas salas;
    private final AlmacenDeSeguridad almacen;
    private final Auditoria auditoria;
    private final Clock reloj;

    @org.springframework.beans.factory.annotation.Autowired
    public VigilanteDeSesiones(Salas salas, AlmacenDeSeguridad almacen, Auditoria auditoria) {
        this(salas, almacen, auditoria, Clock.systemUTC());
    }

    VigilanteDeSesiones(Salas salas, AlmacenDeSeguridad almacen, Auditoria auditoria, Clock reloj) {
        this.salas = salas;
        this.almacen = almacen;
        this.auditoria = auditoria;
        this.reloj = reloj;
    }

    @Scheduled(fixedDelayString = "${cafeorbe.vigilante.intervalo-ms:1000}")
    public void revisar() {
        List<Salas.Conexion> conexiones = salas.todas();
        if (conexiones.isEmpty()) {
            return;
        }
        Set<String> cerradas;
        Set<UUID> bloqueados;
        try {
            cerradas = almacen.cerradas(conexiones.stream().map(c -> c.identidad().jti()).filter(Objects::nonNull)
                    .distinct().toList());
            bloqueados = almacen.bloqueados(conexiones.stream().map(c -> c.identidad().usuarioId()).distinct().toList());
        } catch (RuntimeException e) {
            // Sin Redis igual se cortan las sesiones vencidas, que se deciden con el reloj local.
            log.warn("No se pudo consultar el estado de las sesiones en Redis: {}", e.toString());
            cerradas = Set.of();
            bloqueados = Set.of();
        }
        Instant ahora = Instant.now(reloj);
        for (Salas.Conexion c : conexiones) {
            Identidad id = c.identidad();
            if (!id.expiraEn().isAfter(ahora)) {
                cortar(c, SalaWebSocketHandler.CIERRE_SESION, "sesion_vencida");
            } else if (id.jti() != null && cerradas.contains(id.jti())) {
                cortar(c, SalaWebSocketHandler.CIERRE_SESION, "sesion_cerrada");
            } else if (bloqueados.contains(id.usuarioId())) {
                cortar(c, SalaWebSocketHandler.CIERRE_BLOQUEO, "usuario_bloqueado");
            }
        }
    }

    private void cortar(Salas.Conexion conexion, CloseStatus estado, String motivo) {
        auditoria.evento("conexion_cortada", motivo, conexion.identidad().usuarioId(), null, null);
        try {
            conexion.sesion().close(estado);
        } catch (IOException | RuntimeException e) {
            log.debug("No se pudo cerrar la sesión {}: {}", conexion.sesion().getId(), e.getMessage());
        }
    }
}
