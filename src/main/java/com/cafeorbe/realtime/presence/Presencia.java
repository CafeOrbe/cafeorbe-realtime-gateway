package com.cafeorbe.realtime.presence;

import java.util.UUID;

/** Registro de quién está conectado a cada sala, compartido entre instancias. */
public interface Presencia {

    /** Anota una conexión en la sala. No cuenta: contar recorre toda la sala y no hace falta en cada entrada. */
    void anotar(UUID subastaId, String sesionId, UUID usuarioId);

    /** Retira una conexión de la sala. */
    void retirar(UUID subastaId, String sesionId);

    /** Usuarios distintos conectados a la sala ahora mismo. */
    int contar(UUID subastaId);

    /** Registra una conexión y devuelve cuántos usuarios distintos hay ahora en la sala. */
    default int registrar(UUID subastaId, String sesionId, UUID usuarioId) {
        anotar(subastaId, sesionId, usuarioId);
        return contar(subastaId);
    }

    /** Libera una conexión y devuelve cuántos usuarios distintos quedan en la sala. */
    default int liberar(UUID subastaId, String sesionId) {
        retirar(subastaId, sesionId);
        return contar(subastaId);
    }
}
