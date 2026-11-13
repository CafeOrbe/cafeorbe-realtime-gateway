package com.cafeorbe.realtime.seguridad;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Solo pruebas y una única instancia. */
@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "memoria")
public class AlmacenDeSeguridadEnMemoria implements AlmacenDeSeguridad {

    private record Valor(long numero, Instant vence) {
        boolean vigente() {
            return vence.isAfter(Instant.now());
        }
    }

    private final Map<String, String> tickets = new ConcurrentHashMap<>();
    private final Map<String, Valor> valores = new ConcurrentHashMap<>();

    /** Para las pruebas: lo que identity habría guardado al emitir el ticket. */
    public void guardarTicket(String ticket, String datos) {
        tickets.put(ticket, datos);
    }

    public void cerrarSesion(String jti) {
        valores.put(PREFIJO_CERRADA + jti, new Valor(1, Instant.now().plusSeconds(3600)));
    }

    public void limpiar() {
        tickets.clear();
        valores.clear();
    }

    @Override
    public Optional<String> consumirTicket(String ticket) {
        return Optional.ofNullable(tickets.remove(ticket));
    }

    @Override
    public long contar(String clave, Duration ventana) {
        return valores.compute(clave, (k, v) -> v == null || !v.vigente()
                ? new Valor(1, Instant.now().plus(ventana)) : new Valor(v.numero() + 1, v.vence())).numero();
    }

    @Override
    public void bloquear(UUID usuario, Duration duracion) {
        valores.put(PREFIJO_BLOQUEO + usuario, new Valor(1, Instant.now().plus(duracion)));
    }

    @Override
    public boolean estaBloqueado(UUID usuario) {
        return vigente(PREFIJO_BLOQUEO + usuario);
    }

    @Override
    public long sumarConexion(UUID usuario) {
        return valores.merge(PREFIJO_CONEXIONES + usuario, new Valor(1, Instant.MAX),
                (a, b) -> new Valor(a.numero() + 1, Instant.MAX)).numero();
    }

    @Override
    public void restarConexion(UUID usuario) {
        valores.computeIfPresent(PREFIJO_CONEXIONES + usuario,
                (k, v) -> v.numero() <= 1 ? null : new Valor(v.numero() - 1, Instant.MAX));
    }

    @Override
    public Set<String> cerradas(Collection<String> jtis) {
        return jtis.stream().filter(j -> vigente(PREFIJO_CERRADA + j)).collect(Collectors.toSet());
    }

    @Override
    public Set<UUID> bloqueados(Collection<UUID> usuarios) {
        return usuarios.stream().filter(this::estaBloqueado).collect(Collectors.toSet());
    }

    private boolean vigente(String clave) {
        Valor v = valores.get(clave);
        return v != null && v.vigente();
    }
}
