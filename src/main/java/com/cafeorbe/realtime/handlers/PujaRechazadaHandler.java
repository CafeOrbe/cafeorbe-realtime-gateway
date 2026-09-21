package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.PujaRechazada;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-14: el motivo del rechazo lo ve solo quien pujó. */
@Component
public class PujaRechazadaHandler implements ManejadorDeEvento<PujaRechazada> {

    private final Difusor difusor;

    public PujaRechazadaHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.PUJA_RECHAZADA;
    }

    @Override
    public Class<PujaRechazada> clase() {
        return PujaRechazada.class;
    }

    @Override
    public void manejar(EventoEnvelope<PujaRechazada> evento) {
        difusor.aUsuario(evento.datos().subastaId(), evento.datos().usuarioId(), "PUJA_RECHAZADA", evento.datos());
    }
}
