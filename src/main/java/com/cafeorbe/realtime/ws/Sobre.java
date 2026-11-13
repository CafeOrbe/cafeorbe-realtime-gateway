package com.cafeorbe.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * Lo que viaja por el backplane hacia todas las instancias.
 *
 * @param usuarioId si no es nulo, solo se entrega a las conexiones de ese usuario en la sala
 * @param secuencia REN-01: número de secuencia del evento de auction (id de su outbox); nulo si no viene de auction
 */
public record Sobre(UUID subastaId, UUID usuarioId, String tipo, JsonNode datos, Long secuencia) {

    public Sobre(UUID subastaId, UUID usuarioId, String tipo, JsonNode datos) {
        this(subastaId, usuarioId, tipo, datos, null);
    }
}
