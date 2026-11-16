package com.cafeorbe.realtime.ws;

import java.util.Optional;
import java.util.UUID;

/**
 * REN-01: Coordinador distribuido de pujas y descarte temprano en el borde.
 * Evita la contención concurrente de pujas idénticas sobre auction-service y PostgreSQL.
 */
public interface CoordinadorDePujas {

    /**
     * Intenta adquirir el derecho exclusivo de enviar a auction una puja por este monto.
     *
     * @return true si este comprador es el primero en pujar por este incremento; false si ya hay una en vuelo.
     */
    boolean intentarAdquirirVuelo(UUID subastaId, long monto, UUID usuarioId);

    /** Libera la reserva de vuelo si la puja no pudo enviarse o fue rechazada transitoriamente. */
    void liberarVuelo(UUID subastaId, long monto);

    /** Comprueba si el monto cumple con el mínimo conocido de la subasta. */
    boolean cumpleMinimo(UUID subastaId, long monto);

    /** Obtiene el siguiente mínimo conocido si está registrado. */
    Optional<Long> siguienteMinimo(UUID subastaId);

    /** Actualiza el nuevo monto mínimo requerido para la subasta. */
    void registrarMinimo(UUID subastaId, long nuevoMinimo);
}
