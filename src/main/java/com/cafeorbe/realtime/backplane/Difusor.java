package com.cafeorbe.realtime.backplane;

import com.cafeorbe.realtime.ws.Sobre;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * API que usan los manejadores para difundir mensajes, sin saber si hay una o varias instancias.
 * REN-01: mientras se despacha un evento de auction, cada mensaje que sale lleva su número de secuencia.
 */
@Component
public class Difusor {

    /** El consumidor de eventos es de un solo hilo (concurrency 1), así que basta con un valor por hilo. */
    private static final ThreadLocal<Long> SECUENCIA = new ThreadLocal<>();

    private final Backplane backplane;
    private final ObjectMapper json;

    public Difusor(Backplane backplane, ObjectMapper json) {
        this.backplane = backplane;
        this.json = json;
    }

    /** A todos los conectados a la sala. */
    public void aSala(UUID subastaId, String tipo, Object datos) {
        backplane.publicar(new Sobre(subastaId, null, tipo, json.valueToTree(datos), SECUENCIA.get()));
    }

    /** Solo a las conexiones de un usuario dentro de la sala. */
    public void aUsuario(UUID subastaId, UUID usuarioId, String tipo, Object datos) {
        backplane.publicar(new Sobre(subastaId, usuarioId, tipo, json.valueToTree(datos), SECUENCIA.get()));
    }

    /** Ejecuta la difusión de un evento marcando sus mensajes con la secuencia (puede ser nula). */
    public static void conSecuencia(Long secuencia, Runnable difusion) {
        SECUENCIA.set(secuencia);
        try {
            difusion.run();
        } finally {
            SECUENCIA.remove();
        }
    }
}
