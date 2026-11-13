package com.cafeorbe.realtime.ws;

import com.cafeorbe.contracts.Rol;
import com.cafeorbe.realtime.client.AuctionClient;
import com.cafeorbe.realtime.presence.AvisoDeConectados;
import com.cafeorbe.realtime.presence.Presencia;
import com.cafeorbe.realtime.seguridad.ControlDeAbuso;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.Duration;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import jakarta.annotation.PreDestroy;

/**
 * Una sala WebSocket por subasta. Al entrar registra la presencia y avisa el conteo de conectados (HU-05, HU-15);
 * al recibir {"tipo":"PUJAR","monto":N} reenvía la puja a auction (HU-13), salvo que el usuario esté abusando
 * (SEG-03): ráfagas de pujas, mensajes grandes o usuario bloqueado.
 */
@Component
public class SalaWebSocketHandler extends TextWebSocketHandler {

    /**
     * OBS-01 / REN-01: buckets fijos en los umbrales del escenario (p95 ≤ 800 ms, p99 ≤ 1,5 s). Un histograma
     * completo son ~70 series por réplica y se come el plan gratuito de Grafana Cloud.
     */
    static final Duration[] UMBRALES = {Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(200),
            Duration.ofMillis(400), Duration.ofMillis(800), Duration.ofMillis(1500), Duration.ofSeconds(3)};

    private static final Logger log = LoggerFactory.getLogger(SalaWebSocketHandler.class);
    private static final String ATRIBUTO_SESION_SEGURA = "sesionSegura";
    private static final String ATRIBUTO_COLA_DE_PUJAS = "colaDePujas";

    /** Un hilo virtual por reenvío a auction (Java 21): esperar la respuesta no ocupa un hilo del sistema. */
    private final ExecutorService reenvios = Executors.newVirtualThreadPerTaskExecutor();

    /** Cierres con motivo tipificado (rango 4000-4999 reservado a la aplicación). */
    public static final CloseStatus CIERRE_SESION = new CloseStatus(4401, "Sesión cerrada o vencida");
    public static final CloseStatus CIERRE_BLOQUEO = new CloseStatus(4429, "Usuario bloqueado temporalmente");

    private final Salas salas;
    private final Presencia presencia;
    private final AvisoDeConectados conectados;
    private final AuctionClient auction;
    private final ControlDeAbuso abuso;
    private final ObjectMapper json;
    private final MeterRegistry metricas;
    private final Timer reenvio;

    public SalaWebSocketHandler(Salas salas, Presencia presencia, AvisoDeConectados conectados,
                                AuctionClient auction, ControlDeAbuso abuso, ObjectMapper json, MeterRegistry metricas) {
        this.salas = salas;
        this.presencia = presencia;
        this.conectados = conectados;
        this.auction = auction;
        this.abuso = abuso;
        this.json = json;
        this.metricas = metricas;
        // OBS-01 / REN-01: cuánto tarda auction en procesar una puja reenviada, con p95 y p99.
        this.reenvio = Timer.builder("cafeorbe.realtime.pujas.reenvio")
                .serviceLevelObjectives(UMBRALES)
                .register(metricas);
        metricas.gauge("cafeorbe.realtime.conexiones", salas, s -> s.todas().size());
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession sesion) {
        Identidad identidad = identidad(sesion);
        UUID subastaId = subasta(sesion);
        // sendMessage no es seguro entre hilos: el decorador serializa los envíos y corta a los clientes lentos.
        var segura = new ConcurrentWebSocketSessionDecorator(sesion, 5_000, 64 * 1024);
        sesion.getAttributes().put(ATRIBUTO_SESION_SEGURA, segura);

        salas.agregar(subastaId, new Salas.Conexion(segura, identidad));
        presencia.anotar(subastaId, sesion.getId(), identidad.usuarioId());
        conectados.salaCambio(subastaId);
        log.debug("{} entró a la sala {}", identidad.nombre(), subastaId);
    }

    @Override
    protected void handleTextMessage(WebSocketSession sesion, TextMessage mensaje) {
        Identidad identidad = identidad(sesion);
        UUID subastaId = subasta(sesion);
        String correlacion = UUID.randomUUID().toString();
        MDC.put("correlationId", correlacion);
        try {
            if (abuso.mensajeDemasiadoGrande(mensaje.getPayload().getBytes(StandardCharsets.UTF_8).length)) {
                abuso.infraccion(identidad.usuarioId(), ControlDeAbuso.MOTIVO_TAMANO, ip(sesion), correlacion);
                cerrar(sesion, CloseStatus.TOO_BIG_TO_PROCESS);
                return;
            }
            JsonNode entrada = json.readTree(mensaje.getPayload());
            switch (entrada.path("tipo").asText("")) {
                case "PING" -> responder(sesion, subastaId, "PONG", Map.of());
                case "PUJAR" -> pujar(sesion, identidad, subastaId, entrada, correlacion);
                default -> responder(sesion, subastaId, "ERROR", Map.of("mensaje", "Mensaje no reconocido"));
            }
        } catch (Exception e) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "Mensaje no válido"));
        } finally {
            MDC.remove("correlationId");
        }
    }

    private void pujar(WebSocketSession sesion, Identidad identidad, UUID subastaId, JsonNode entrada,
                       String correlacion) {
        if (identidad.rol() != Rol.COMPRADOR) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "No autorizado"));
            return;
        }
        if (!entrada.path("monto").canConvertToLong()) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "El monto de la puja no es válido"));
            return;
        }
        if (abuso.estaBloqueado(identidad.usuarioId())) {
            cerrar(sesion, CIERRE_BLOQUEO);
            return;
        }
        if (!abuso.pujaPermitida(identidad.usuarioId())) {
            metricas.counter("cafeorbe.realtime.pujas.rechazadas_por_limite").increment();
            responder(sesion, subastaId, "ERROR", Map.of("motivo", "LIMITE_DE_PUJAS",
                    "mensaje", "Estás pujando demasiado rápido, espera un momento"));
            if (abuso.infraccion(identidad.usuarioId(), ControlDeAbuso.MOTIVO_PUJAS, ip(sesion), correlacion)) {
                cerrar(sesion, CIERRE_BLOQUEO);
            }
            return;
        }
        long monto = entrada.get("monto").asLong();
        metricas.counter("cafeorbe.realtime.pujas.recibidas").increment();
        log.atInfo().addKeyValue("subastaId", subastaId).addKeyValue("usuario", identidad.usuarioId())
                .addKeyValue("monto", monto).log("Puja recibida por WebSocket");
        encolar(sesion, () -> {
            MDC.put("correlationId", correlacion);
            try {
                reenviar(sesion, identidad, subastaId, monto, correlacion);
            } finally {
                MDC.remove("correlationId");
            }
        });
    }

    /**
     * SEG-03: el reenvío a auction no bloquea la lectura de la conexión. Si lo hiciera, cada mensaje esperaría a
     * que auction respondiera el anterior (cientos de ms con la base en otra región), las pujas entrarían más
     * lento de lo que el atacante las manda y el límite por segundo nunca se alcanzaría. Así el límite se mide al
     * llegar cada mensaje, y las pujas aceptadas de una misma conexión siguen llegando a auction en orden: cada
     * una espera a la anterior en un hilo virtual. Lo pendiente está acotado por el mismo límite.
     */
    private void encolar(WebSocketSession sesion, Runnable reenvio) {
        synchronized (sesion) {
            CompletableFuture<?> anterior = (CompletableFuture<?>) sesion.getAttributes()
                    .getOrDefault(ATRIBUTO_COLA_DE_PUJAS, CompletableFuture.completedFuture(null));
            // exceptionally: una puja que falló no deja trabadas las siguientes de la misma conexión.
            sesion.getAttributes().put(ATRIBUTO_COLA_DE_PUJAS,
                    anterior.exceptionally(error -> null).thenRunAsync(reenvio, reenvios));
        }
    }

    private void reenviar(WebSocketSession sesion, Identidad identidad, UUID subastaId, long monto, String correlacion) {
        if (abuso.estaBloqueado(identidad.usuarioId())) {
            return; // quedó bloqueado mientras esta puja esperaba turno: no llega a auction
        }
        try {
            var muestra = Timer.start();
            java.util.Optional<JsonNode> rechazo;
            try {
                rechazo = auction.pujar(subastaId, identidad, monto, correlacion);
            } finally {
                muestra.stop(reenvio);
            }
            if (rechazo.isPresent()) {
                responder(sesion, subastaId, "PUJA_RECHAZADA", rechazoDirecto(subastaId, identidad, rechazo.get()));
            }
        } catch (AuctionClient.PujaNoEnviadaException e) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", e.getMessage()));
        } catch (RuntimeException e) {
            // Sin esto la excepción quedaría en el CompletableFuture y quien pujó no sabría nada.
            log.warn("No se pudo reenviar la puja a auction: {}", e.getMessage());
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "No se pudo registrar la puja, intenta de nuevo"));
        }
    }

    /** Al apagar, espera a que terminen las pujas ya aceptadas por el límite antes de soltar la réplica. */
    @PreDestroy
    void terminarReenvios() {
        reenvios.close();
    }

    /** Mismo formato que el evento PujaRechazada, para que el navegador lo trate igual. */
    private static Map<String, Object> rechazoDirecto(UUID subastaId, Identidad identidad, JsonNode respuesta) {
        Map<String, Object> datos = new LinkedHashMap<>();
        datos.put("subastaId", subastaId);
        datos.put("usuarioId", identidad.usuarioId());
        datos.put("montoIntentado", respuesta.path("monto").asLong());
        datos.put("motivo", respuesta.path("motivo").asText(""));
        datos.put("mensaje", respuesta.path("mensaje").asText(""));
        return datos;
    }

    @Override
    public void afterConnectionClosed(WebSocketSession sesion, CloseStatus estado) {
        UUID subastaId = subasta(sesion);
        salas.quitar(subastaId, sesion.getId());
        presencia.retirar(subastaId, sesion.getId());
        conectados.salaCambio(subastaId);
        if (Boolean.TRUE.equals(sesion.getAttributes().remove(HandshakeConTicket.ATRIBUTO_CONEXION_CONTADA))) {
            abuso.cerrarConexion(identidad(sesion).usuarioId());
        }
    }

    @Override
    public void handleTransportError(WebSocketSession sesion, Throwable error) {
        log.debug("Error de transporte en la sesión {}: {}", sesion.getId(), error.getMessage());
    }

    private void responder(WebSocketSession sesion, UUID subastaId, String tipo, Object datos) {
        try {
            salas.enviar(segura(sesion), json.writeValueAsString(new Mensaje(tipo, subastaId, json.valueToTree(datos))));
        } catch (Exception e) {
            log.debug("No se pudo responder a {}: {}", sesion.getId(), e.getMessage());
        }
    }

    private static void cerrar(WebSocketSession sesion, CloseStatus estado) {
        try {
            segura(sesion).close(estado);
        } catch (IOException | RuntimeException e) {
            log.debug("No se pudo cerrar la sesión {}: {}", sesion.getId(), e.getMessage());
        }
    }

    private static WebSocketSession segura(WebSocketSession sesion) {
        return (WebSocketSession) sesion.getAttributes().getOrDefault(ATRIBUTO_SESION_SEGURA, sesion);
    }

    private static String ip(WebSocketSession sesion) {
        return (String) sesion.getAttributes().get(HandshakeConTicket.ATRIBUTO_IP);
    }

    private static Identidad identidad(WebSocketSession sesion) {
        return (Identidad) sesion.getAttributes().get(HandshakeConTicket.ATRIBUTO_IDENTIDAD);
    }

    private static UUID subasta(WebSocketSession sesion) {
        return (UUID) sesion.getAttributes().get(HandshakeConTicket.ATRIBUTO_SUBASTA);
    }
}
