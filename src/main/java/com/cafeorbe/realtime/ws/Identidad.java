package com.cafeorbe.realtime.ws;

import com.cafeorbe.contracts.Rol;

import java.time.Instant;
import java.util.UUID;

/**
 * Usuario autenticado de una conexión WebSocket, tomado del ticket que emitió identity (SEG-04).
 *
 * @param jti      identificador de la sesión: si se cierra, la conexión se corta (SEG-05)
 * @param expiraEn vencimiento del token de sesión: al llegar, la conexión se corta (SEG-05)
 */
public record Identidad(UUID usuarioId, String nombre, Rol rol, String jti, Instant expiraEn) {

    /** Identidad sin sesión asociada (pruebas y llamadas internas). */
    public Identidad(UUID usuarioId, String nombre, Rol rol) {
        this(usuarioId, nombre, rol, null, Instant.MAX);
    }
}
