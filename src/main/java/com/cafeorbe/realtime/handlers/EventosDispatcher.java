package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Elige el manejador según el tipo de evento y le entrega el sobre ya deserializado. */
@Component
public class EventosDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventosDispatcher.class);

    private final Map<String, ManejadorDeEvento<?>> manejadores;
    private final ObjectMapper json;

    public EventosDispatcher(List<ManejadorDeEvento<?>> manejadores, ObjectMapper json) {
        this.manejadores = manejadores.stream()
                .collect(Collectors.toMap(ManejadorDeEvento::tipoDeEvento, Function.identity()));
        this.json = json;
    }

    public void despachar(String tipoDeEvento, byte[] cuerpo) {
        ManejadorDeEvento<?> manejador = manejadores.get(tipoDeEvento);
        if (manejador == null) {
            log.debug("Evento {} sin manejador, se ignora", tipoDeEvento);
            return;
        }
        try {
            manejar(manejador, cuerpo);
        } catch (IOException e) {
            // Un mensaje ilegible nunca se va a poder procesar: se descarta en vez de reintentarlo sin fin.
            log.error("No se pudo leer el evento {}: {}", tipoDeEvento, e.getMessage());
        }
    }

    private <T> void manejar(ManejadorDeEvento<T> manejador, byte[] cuerpo) throws IOException {
        JavaType tipo = json.getTypeFactory().constructParametricType(EventoEnvelope.class, manejador.clase());
        EventoEnvelope<T> evento = json.readValue(cuerpo, tipo);
        manejador.manejar(evento);
    }
}
