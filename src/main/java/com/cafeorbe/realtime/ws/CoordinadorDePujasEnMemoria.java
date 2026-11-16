package com.cafeorbe.realtime.ws;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementación en memoria de CoordinadorDePujas para pruebas y modo aislado.
 */
@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "memoria")
public class CoordinadorDePujasEnMemoria implements CoordinadorDePujas {

    private final ConcurrentHashMap<UUID, Long> minimos = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> vuelos = new ConcurrentHashMap<>();

    @Override
    public boolean intentarAdquirirVuelo(UUID subastaId, long monto, UUID usuarioId) {
        String clave = subastaId + ":" + monto;
        long ahora = System.nanoTime();
        Long expira = vuelos.get(clave);
        if (expira != null && expira - ahora > 0) {
            return false;
        }
        vuelos.put(clave, ahora + 3_000_000_000L);
        return true;
    }

    @Override
    public void liberarVuelo(UUID subastaId, long monto) {
        vuelos.remove(subastaId + ":" + monto);
    }

    @Override
    public boolean cumpleMinimo(UUID subastaId, long monto) {
        Long min = minimos.get(subastaId);
        return min == null || monto >= min;
    }

    @Override
    public Optional<Long> siguienteMinimo(UUID subastaId) {
        return Optional.ofNullable(minimos.get(subastaId));
    }

    @Override
    public void registrarMinimo(UUID subastaId, long nuevoMinimo) {
        minimos.compute(subastaId, (k, v) -> v == null ? nuevoMinimo : Math.max(v, nuevoMinimo));
    }
}
