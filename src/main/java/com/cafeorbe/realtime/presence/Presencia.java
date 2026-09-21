package com.cafeorbe.realtime.presence;

import java.util.UUID;

/** Registro de quién está conectado a cada sala, compartido entre instancias. */
public interface Presencia {

    /** Registra una conexión y devuelve cuántos usuarios distintos hay ahora en la sala. */
    int registrar(UUID subastaId, String sesionId, UUID usuarioId);

    /** Libera una conexión y devuelve cuántos usuarios distintos quedan en la sala. */
    int liberar(UUID subastaId, String sesionId);
}
