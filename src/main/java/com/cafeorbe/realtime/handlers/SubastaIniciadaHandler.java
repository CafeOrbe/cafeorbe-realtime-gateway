package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.SubastaIniciada;
import com.cafeorbe.realtime.backplane.Difusor;
import com.cafeorbe.realtime.ws.CoordinadorDePujas;
import org.springframework.stereotype.Component;

/** HU-12: habilita las pujas y el temporizador en las pantallas de la sala. */
@Component
public class SubastaIniciadaHandler implements ManejadorDeEvento<SubastaIniciada> {

    private final Difusor difusor;
    private final CoordinadorDePujas coordinador;

    public SubastaIniciadaHandler(Difusor difusor, CoordinadorDePujas coordinador) {
        this.difusor = difusor;
        this.coordinador = coordinador;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.SUBASTA_INICIADA;
    }

    @Override
    public Class<SubastaIniciada> clase() {
        return SubastaIniciada.class;
    }

    @Override
    public void manejar(EventoEnvelope<SubastaIniciada> evento) {
        coordinador.registrarMinimo(evento.datos().subastaId(), evento.datos().precioBase());
        difusor.aSala(evento.datos().subastaId(), "SUBASTA_INICIADA", evento.datos());
    }
}
