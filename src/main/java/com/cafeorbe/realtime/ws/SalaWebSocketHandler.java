package com.cafeorbe.realtime.ws;

import com.cafeorbe.contracts.Rol;
import com.cafeorbe.realtime.backplane.Difusor;
import com.cafeorbe.realtime.client.AuctionClient;
import com.cafeorbe.realtime.presence.Presencia;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;
import java.util.UUID;

/**
 * Una sala WebSocket por subasta. Al entrar registra la presencia y avisa el conteo de conectados (HU-05, HU-15);
 * al recibir {"tipo":"PUJAR","monto":N} reenvía la puja a auction (HU-13).
 */
@Component
public class SalaWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(SalaWebSocketHandler.class);
    private static final String ATRIBUTO_SESION_SEGURA = "sesionSegura";

    private final Salas salas;
    private final Presencia presencia;
    private final Difusor difusor;
    private final AuctionClient auction;
    private final ObjectMapper json;

    public SalaWebSocketHandler(Salas salas, Presencia presencia, Difusor difusor, AuctionClient auction,
                                ObjectMapper json) {
        this.salas = salas;
        this.presencia = presencia;
        this.difusor = difusor;
        this.auction = auction;
        this.json = json;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession sesion) {
        Identidad identidad = identidad(sesion);
        UUID subastaId = subasta(sesion);
        // sendMessage no es seguro entre hilos: el decorador serializa los envíos y corta a los clientes lentos.
        var segura = new ConcurrentWebSocketSessionDecorator(sesion, 5_000, 64 * 1024);
        sesion.getAttributes().put(ATRIBUTO_SESION_SEGURA, segura);

        salas.agregar(subastaId, new Salas.Conexion(segura, identidad));
        int conectados = presencia.registrar(subastaId, sesion.getId(), identidad.usuarioId());
        difusor.aSala(subastaId, "CONECTADOS", Map.of("conectados", conectados));
        log.debug("{} entró a la sala {} ({} conectados)", identidad.nombre(), subastaId, conectados);
    }

    @Override
    protected void handleTextMessage(WebSocketSession sesion, TextMessage mensaje) {
        Identidad identidad = identidad(sesion);
        UUID subastaId = subasta(sesion);
        try {
            JsonNode entrada = json.readTree(mensaje.getPayload());
            switch (entrada.path("tipo").asText("")) {
                case "PING" -> responder(sesion, subastaId, "PONG", Map.of());
                case "PUJAR" -> pujar(sesion, identidad, subastaId, entrada);
                default -> responder(sesion, subastaId, "ERROR", Map.of("mensaje", "Mensaje no reconocido"));
            }
        } catch (Exception e) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "Mensaje no válido"));
        }
    }

    private void pujar(WebSocketSession sesion, Identidad identidad, UUID subastaId, JsonNode entrada) {
        if (identidad.rol() != Rol.COMPRADOR) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "No autorizado"));
            return;
        }
        if (!entrada.path("monto").canConvertToLong()) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", "El monto de la puja no es válido"));
            return;
        }
        try {
            auction.pujar(subastaId, identidad, entrada.get("monto").asLong());
        } catch (AuctionClient.PujaNoEnviadaException e) {
            responder(sesion, subastaId, "ERROR", Map.of("mensaje", e.getMessage()));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession sesion, CloseStatus estado) {
        UUID subastaId = subasta(sesion);
        salas.quitar(subastaId, sesion.getId());
        int conectados = presencia.liberar(subastaId, sesion.getId());
        difusor.aSala(subastaId, "CONECTADOS", Map.of("conectados", conectados));
    }

    @Override
    public void handleTransportError(WebSocketSession sesion, Throwable error) {
        log.debug("Error de transporte en la sesión {}: {}", sesion.getId(), error.getMessage());
    }

    private void responder(WebSocketSession sesion, UUID subastaId, String tipo, Object datos) {
        try {
            var segura = (WebSocketSession) sesion.getAttributes().getOrDefault(ATRIBUTO_SESION_SEGURA, sesion);
            salas.enviar(segura, json.writeValueAsString(new Mensaje(tipo, subastaId, json.valueToTree(datos))));
        } catch (Exception e) {
            log.debug("No se pudo responder a {}: {}", sesion.getId(), e.getMessage());
        }
    }

    private static Identidad identidad(WebSocketSession sesion) {
        return (Identidad) sesion.getAttributes().get(HandshakeConToken.ATRIBUTO_IDENTIDAD);
    }

    private static UUID subasta(WebSocketSession sesion) {
        return (UUID) sesion.getAttributes().get(HandshakeConToken.ATRIBUTO_SUBASTA);
    }
}
