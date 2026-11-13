package com.cafeorbe.realtime.seguridad;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Estado de seguridad compartido entre réplicas (Redis). Un atacante puede repartir sus conexiones entre las
 * réplicas, por eso los límites no pueden contarse solo en memoria.
 */
public interface AlmacenDeSeguridad {

    String PREFIJO_TICKET = "cafeorbe:ws:ticket:";
    String PREFIJO_CERRADA = "cafeorbe:sesion:cerrada:";
    String PREFIJO_BLOQUEO = "cafeorbe:seg:bloqueado:";
    String PREFIJO_CONEXIONES = "cafeorbe:ws:conexiones:";

    /** SEG-04: lee y borra el ticket en una sola operación (GET y DEL atómicos). Un segundo intento ya no lo encuentra. */
    Optional<String> consumirTicket(String ticket);

    /** Suma 1 a la clave y devuelve el total; la clave vence al terminar la ventana. */
    long contar(String clave, Duration ventana);

    void bloquear(UUID usuario, Duration duracion);

    boolean estaBloqueado(UUID usuario);

    /** @return conexiones abiertas del usuario contando la nueva */
    long sumarConexion(UUID usuario);

    void restarConexion(UUID usuario);

    /** SEG-05: de estas sesiones, cuáles ya se cerraron. */
    Set<String> cerradas(Collection<String> jtis);

    /** SEG-03: de estos usuarios, cuáles están bloqueados. */
    Set<UUID> bloqueados(Collection<UUID> usuarios);
}
