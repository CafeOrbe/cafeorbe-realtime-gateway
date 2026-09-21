package com.cafeorbe.realtime.ws;

import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;
import java.util.UUID;

/**
 * Rechaza la conexión si el token no es válido. Los navegadores no pueden enviar cabeceras en un
 * WebSocket, por eso el token viaja en la URL: {@code /ws/salas/{subastaId}?token=...}.
 */
@Component
public class HandshakeConToken implements HandshakeInterceptor {

    static final String ATRIBUTO_IDENTIDAD = "identidad";
    static final String ATRIBUTO_SUBASTA = "subastaId";

    private final ValidadorDeToken validador;

    public HandshakeConToken(ValidadorDeToken validador) {
        this.validador = validador;
    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest peticion, ServerHttpResponse respuesta,
                                   WebSocketHandler manejador, Map<String, Object> atributos) {
        var uri = peticion.getURI();
        String ruta = uri.getPath();
        UUID subastaId;
        try {
            subastaId = UUID.fromString(ruta.substring(ruta.lastIndexOf('/') + 1));
        } catch (IllegalArgumentException e) {
            respuesta.setStatusCode(HttpStatus.BAD_REQUEST);
            return false;
        }
        String token = UriComponentsBuilder.fromUri(uri).build().getQueryParams().getFirst("token");
        var identidad = validador.validar(token);
        if (identidad.isEmpty()) {
            respuesta.setStatusCode(HttpStatus.UNAUTHORIZED);
            return false;
        }
        atributos.put(ATRIBUTO_IDENTIDAD, identidad.get());
        atributos.put(ATRIBUTO_SUBASTA, subastaId);
        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest peticion, ServerHttpResponse respuesta,
                               WebSocketHandler manejador, Exception excepcion) {
        // Nada que hacer después del handshake.
    }
}
