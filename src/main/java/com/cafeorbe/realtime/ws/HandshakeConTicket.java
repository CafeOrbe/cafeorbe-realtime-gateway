package com.cafeorbe.realtime.ws;

import com.cafeorbe.contracts.Rol;
import com.cafeorbe.realtime.client.AuctionClient;
import com.cafeorbe.realtime.seguridad.AlmacenDeSeguridad;
import com.cafeorbe.realtime.seguridad.Auditoria;
import com.cafeorbe.realtime.seguridad.ControlDeAbuso;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * SEG-04: la conexión se abre con un ticket de un solo uso, nunca con el token de sesión:
 * {@code /ws/salas/{subastaId}?ticket=...}. Los navegadores no pueden enviar cabeceras en un WebSocket, así que
 * algo tiene que ir en la URL; si esa URL queda en un log, el ticket ya se consumió y venció a los pocos segundos.
 * <p>
 * Antes de aceptar la conexión también comprueba que la sesión no esté cerrada ni vencida (SEG-05) y que el
 * usuario no esté bloqueado ni supere su máximo de conexiones (SEG-03). Cada rechazo queda en la auditoría.
 */
@Component
public class HandshakeConTicket implements HandshakeInterceptor {

    static final String ATRIBUTO_IDENTIDAD = "identidad";
    static final String ATRIBUTO_SUBASTA = "subastaId";
    static final String ATRIBUTO_IP = "ip";
    static final String ATRIBUTO_CONEXION_CONTADA = "conexionContada";

    private final AlmacenDeSeguridad almacen;
    private final ControlDeAbuso abuso;
    private final Auditoria auditoria;
    private final AuctionClient auction;
    private final ObjectMapper json;
    private final Clock reloj;

    public HandshakeConTicket(AlmacenDeSeguridad almacen, ControlDeAbuso abuso, Auditoria auditoria,
                              AuctionClient auction, ObjectMapper json) {
        this.almacen = almacen;
        this.abuso = abuso;
        this.auditoria = auditoria;
        this.auction = auction;
        this.json = json;
        this.reloj = Clock.systemUTC();
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest peticion, ServerHttpResponse respuesta,
                                   WebSocketHandler manejador, Map<String, Object> atributos) {
        var uri = peticion.getURI();
        String ruta = uri.getPath();
        String ip = ipDe(peticion);
        String correlacion = UUID.randomUUID().toString();
        UUID subastaId;
        try {
            subastaId = UUID.fromString(ruta.substring(ruta.lastIndexOf('/') + 1));
        } catch (IllegalArgumentException e) {
            respuesta.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        String ticket = UriComponentsBuilder.fromUri(uri).build().getQueryParams().getFirst("ticket");
        Optional<Identidad> leida = ticket == null || ticket.isBlank()
                ? Optional.empty()
                : almacen.consumirTicket(ticket).flatMap(this::leer);
        if (leida.isEmpty()) {
            auditoria.evento("conexion_rechazada", "ticket_invalido_usado_o_vencido", null, ip, correlacion);
            respuesta.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
        Identidad identidad = leida.get();

        if (!identidad.expiraEn().isAfter(Instant.now(reloj))) {
            return rechazar(respuesta, HttpStatus.UNAUTHORIZED, "sesion_vencida", identidad, ip, correlacion);
        }
        if (!almacen.cerradas(java.util.List.of(identidad.jti())).isEmpty()) {
            return rechazar(respuesta, HttpStatus.UNAUTHORIZED, "sesion_cerrada", identidad, ip, correlacion);
        }
        if (abuso.estaBloqueado(identidad.usuarioId())) {
            return rechazar(respuesta, HttpStatus.TOO_MANY_REQUESTS, "usuario_bloqueado", identidad, ip, correlacion);
        }
        // Sin esto cualquier UUID abría una sala y dejaba presencia en Redis para subastas inexistentes.
        if (auction.noExiste(subastaId, identidad)) {
            respuesta.setStatusCode(HttpStatus.NOT_FOUND);
            return false;
        }
        if (!abuso.abrirConexion(identidad.usuarioId())) {
            abuso.infraccion(identidad.usuarioId(), ControlDeAbuso.MOTIVO_CONEXIONES, ip, correlacion);
            respuesta.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return false;
        }
        atributos.put(ATRIBUTO_IDENTIDAD, identidad);
        atributos.put(ATRIBUTO_SUBASTA, subastaId);
        atributos.put(ATRIBUTO_IP, ip);
        atributos.put(ATRIBUTO_CONEXION_CONTADA, Boolean.TRUE);
        return true;
    }

    private boolean rechazar(ServerHttpResponse respuesta, HttpStatus estado, String motivo, Identidad identidad,
                             String ip, String correlacion) {
        auditoria.evento("conexion_rechazada", motivo, identidad.usuarioId(), ip, correlacion);
        respuesta.setStatusCode(estado);
        return false;
    }

    /** El ticket lo escribió identity: {@code {usuarioId, nombre, rol, jti, expiraEn}}. */
    private Optional<Identidad> leer(String datos) {
        try {
            JsonNode n = json.readTree(datos);
            String jti = n.path("jti").asText("");
            if (jti.isBlank() || !n.path("expiraEn").canConvertToLong()) {
                return Optional.empty();
            }
            return Optional.of(new Identidad(UUID.fromString(n.path("usuarioId").asText()), n.path("nombre").asText(),
                    Rol.valueOf(n.path("rol").asText()), jti, Instant.ofEpochSecond(n.path("expiraEn").asLong())));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static String ipDe(ServerHttpRequest peticion) {
        String reenviada = peticion.getHeaders().getFirst("X-Forwarded-For");
        if (reenviada != null && !reenviada.isBlank()) {
            return reenviada.split(",")[0].trim();
        }
        var remota = peticion.getRemoteAddress();
        return remota == null || remota.getAddress() == null ? null : remota.getAddress().getHostAddress();
    }

    @Override
    public void afterHandshake(ServerHttpRequest peticion, ServerHttpResponse respuesta,
                               WebSocketHandler manejador, Exception excepcion) {
        // Nada que hacer después del handshake.
    }
}
