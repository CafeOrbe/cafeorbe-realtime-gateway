package com.cafeorbe.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * Lo que viaja por el backplane hacia todas las instancias.
 *
 * @param usuarioId si no es nulo, solo se entrega a las conexiones de ese usuario en la sala
 */
public record Sobre(UUID subastaId, UUID usuarioId, String tipo, JsonNode datos) {
}
