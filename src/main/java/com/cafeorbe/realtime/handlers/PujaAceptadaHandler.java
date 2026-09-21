package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.PujaAceptada;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-13 y HU-16: nuevo líder y precio para todos los conectados a la sala. */
@Component
public class PujaAceptadaHandler implements ManejadorDeEvento<PujaAceptada> {

    private final Difusor difusor;

    public PujaAceptadaHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.PUJA_ACEPTADA;
    }

    @Override
    public Class<PujaAceptada> clase() {
        return PujaAceptada.class;
    }

    @Override
    public void manejar(EventoEnvelope<PujaAceptada> evento) {
        difusor.aSala(evento.datos().subastaId(), "PUJA_ACEPTADA", evento.datos());
    }
}
