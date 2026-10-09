package com.cafeorbe.realtime.presence;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Para pruebas o una sola instancia. */
@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "memoria")
public class PresenciaEnMemoria implements Presencia {

    private final Map<UUID, Map<String, UUID>> salas = new HashMap<>();

    @Override
    public synchronized void anotar(UUID subastaId, String sesionId, UUID usuarioId) {
        salas.computeIfAbsent(subastaId, k -> new HashMap<>()).put(sesionId, usuarioId);
    }

    @Override
    public synchronized void retirar(UUID subastaId, String sesionId) {
        var sala = salas.get(subastaId);
        if (sala != null) {
            sala.remove(sesionId);
        }
    }

    @Override
    public synchronized int contar(UUID subastaId) {
        return (int) salas.getOrDefault(subastaId, Map.of()).values().stream().distinct().count();
    }
}
