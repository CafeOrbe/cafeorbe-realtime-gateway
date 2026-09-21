package com.cafeorbe.realtime.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Conexiones WebSocket de ESTA instancia, agrupadas por sala (una sala por subasta). */
@Component
public class Salas {

    private static final Logger log = LoggerFactory.getLogger(Salas.class);

    public record Conexion(WebSocketSession sesion, Identidad identidad) {
    }

    private final Map<UUID, Map<String, Conexion>> salas = new ConcurrentHashMap<>();
    private final ObjectMapper json;

    public Salas(ObjectMapper json) {
        this.json = json;
    }

    public void agregar(UUID subastaId, Conexion conexion) {
        salas.computeIfAbsent(subastaId, k -> new ConcurrentHashMap<>()).put(conexion.sesion().getId(), conexion);
    }

    public void quitar(UUID subastaId, String sesionId) {
        salas.computeIfPresent(subastaId, (k, conexiones) -> {
            conexiones.remove(sesionId);
            return conexiones.isEmpty() ? null : conexiones;
        });
    }

    public int conexionesLocales(UUID subastaId) {
        return salas.getOrDefault(subastaId, Map.of()).size();
    }

    /** Entrega un sobre a las conexiones locales que corresponden: toda la sala o solo un usuario. */
    public void entregar(Sobre sobre) {
        var conexiones = salas.get(sobre.subastaId());
        if (conexiones == null) {
            return;
        }
        String texto;
        try {
            texto = json.writeValueAsString(new Mensaje(sobre.tipo(), sobre.subastaId(), sobre.datos()));
        } catch (JsonProcessingException e) {
            log.error("No se pudo serializar el mensaje {}", sobre.tipo(), e);
            return;
        }
        for (Conexion c : conexiones.values()) {
            if (sobre.usuarioId() == null || sobre.usuarioId().equals(c.identidad().usuarioId())) {
                enviar(c.sesion(), texto);
            }
        }
    }

    public void enviar(WebSocketSession sesion, String texto) {
        try {
            if (sesion.isOpen()) {
                sesion.sendMessage(new TextMessage(texto));
            }
        } catch (IOException | RuntimeException e) {
            // Un cliente lento o caído no debe afectar a los demás.
            log.debug("No se pudo enviar a la sesión {}: {}", sesion.getId(), e.getMessage());
        }
    }
}
