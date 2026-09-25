package com.cafeorbe.realtime.client;

import com.cafeorbe.contracts.Cabeceras;
import com.cafeorbe.contracts.dto.PujaSolicitud;
import com.cafeorbe.realtime.ws.Identidad;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Reenvía a auction las pujas que llegan por WebSocket. El realtime-gateway no decide nada: si la puja
 * se acepta o se rechaza, el resultado llega a la sala por los eventos PujaAceptada / PujaRechazada.
 */
@Component
public class AuctionClient {

    /** Fallo que sí debe ver quien pujó (subasta inexistente, sin permiso, auction caído). */
    public static class PujaNoEnviadaException extends RuntimeException {
        public PujaNoEnviadaException(String mensaje) {
            super(mensaje);
        }
    }

    private final RestClient cliente;
    private final ObjectMapper json;

    public AuctionClient(@Value("${cafeorbe.auction.url}") String url,
                         @Value("${cafeorbe.auction.connect-timeout-ms}") int conexionMs,
                         @Value("${cafeorbe.auction.read-timeout-ms}") int lecturaMs,
                         ObjectMapper json) {
        var fabrica = new SimpleClientHttpRequestFactory();
        fabrica.setConnectTimeout(conexionMs);
        fabrica.setReadTimeout(lecturaMs);
        this.cliente = RestClient.builder().baseUrl(url).requestFactory(fabrica).build();
        this.json = json;
    }

    /** HTTP 200 (aceptada) y 422 (rechazada con motivo) son respuestas normales: el motivo llega por evento. */
    public void pujar(UUID subastaId, Identidad usuario, long monto) {
        try {
            cliente.post().uri("/api/subastas/{id}/pujas", subastaId)
                    .header(Cabeceras.USUARIO_ID, usuario.usuarioId().toString())
                    .header(Cabeceras.USUARIO_NOMBRE, URLEncoder.encode(usuario.nombre(), StandardCharsets.UTF_8))
                    .header(Cabeceras.USUARIO_ROL, usuario.rol().name())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new PujaSolicitud(monto))
                    .retrieve()
                    .onStatus(estado -> estado.value() == HttpStatus.UNPROCESSABLE_ENTITY.value(), (req, res) -> {
                    })
                    .toBodilessEntity();
        } catch (RestClientResponseException e) {
            throw new PujaNoEnviadaException(mensajeDe(e));
        } catch (RestClientException e) {
            throw new PujaNoEnviadaException("No se pudo enviar la puja, intenta de nuevo");
        }
    }

    /**
     * HU-05: {@code true} solo si auction confirma que la subasta no existe (HTTP 404). Si auction no responde
     * se deja entrar: la sala carga el detalle por REST y el WebSocket se resincroniza al reconectar.
     */
    public boolean noExiste(UUID subastaId, Identidad usuario) {
        try {
            cliente.get().uri("/api/subastas/{id}", subastaId)
                    .header(Cabeceras.USUARIO_ID, usuario.usuarioId().toString())
                    .header(Cabeceras.USUARIO_NOMBRE, URLEncoder.encode(usuario.nombre(), StandardCharsets.UTF_8))
                    .header(Cabeceras.USUARIO_ROL, usuario.rol().name())
                    .retrieve()
                    .toBodilessEntity();
            return false;
        } catch (RestClientResponseException e) {
            return e.getStatusCode().value() == HttpStatus.NOT_FOUND.value();
        } catch (RestClientException e) {
            return false;
        }
    }

    private String mensajeDe(RestClientResponseException e) {
        try {
            String mensaje = json.readTree(e.getResponseBodyAsString()).path("mensaje").asText("");
            if (!mensaje.isBlank()) {
                return mensaje;
            }
        } catch (Exception ignorada) {
            // Cuerpo que no es JSON: se usa el mensaje genérico.
        }
        return "No se pudo enviar la puja, intenta de nuevo";
    }
}
