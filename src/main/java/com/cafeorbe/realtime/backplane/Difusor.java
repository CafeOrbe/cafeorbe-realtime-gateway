package com.cafeorbe.realtime.backplane;

import com.cafeorbe.realtime.ws.Sobre;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.UUID;

/** API que usan los manejadores para difundir mensajes, sin saber si hay una o varias instancias. */
@Component
public class Difusor {

    private final Backplane backplane;
    private final ObjectMapper json;

    public Difusor(Backplane backplane, ObjectMapper json) {
        this.backplane = backplane;
        this.json = json;
    }

    /** A todos los conectados a la sala. */
    public void aSala(UUID subastaId, String tipo, Object datos) {
        backplane.publicar(new Sobre(subastaId, null, tipo, json.valueToTree(datos)));
    }

    /** Solo a las conexiones de un usuario dentro de la sala. */
    public void aUsuario(UUID subastaId, UUID usuarioId, String tipo, Object datos) {
        backplane.publicar(new Sobre(subastaId, usuarioId, tipo, json.valueToTree(datos)));
    }
}
