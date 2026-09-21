package com.cafeorbe.realtime.ws;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.UUID;

/** Mensaje que recibe el navegador: {@code {"tipo":"PUJA_ACEPTADA","subastaId":"...","datos":{...}}}. */
public record Mensaje(String tipo, UUID subastaId, JsonNode datos) {
}
