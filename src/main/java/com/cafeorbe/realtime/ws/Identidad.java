package com.cafeorbe.realtime.ws;

import com.cafeorbe.contracts.Rol;

import java.util.UUID;

/** Usuario autenticado de una conexión WebSocket, tomado del token de sesión. */
public record Identidad(UUID usuarioId, String nombre, Rol rol) {
}
