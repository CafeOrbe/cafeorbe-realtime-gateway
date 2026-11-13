package com.cafeorbe.realtime.ws;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

/**
 * Mensaje que recibe el navegador: {@code {"tipo":"PUJA_ACEPTADA","subastaId":"...","datos":{...},"secuencia":N}}.
 * REN-01: {@code secuencia} crece con cada evento de auction; el navegador descarta lo repetido o atrasado.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Mensaje(String tipo, UUID subastaId, JsonNode datos, Long secuencia) {

    public Mensaje(String tipo, UUID subastaId, JsonNode datos) {
        this(tipo, subastaId, datos, null);
    }
}
