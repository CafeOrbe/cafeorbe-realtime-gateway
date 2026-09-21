package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.TransmisionIniciada;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-11: indicador EN VIVO para los conectados. */
@Component
public class TransmisionIniciadaHandler implements ManejadorDeEvento<TransmisionIniciada> {

    private final Difusor difusor;

    public TransmisionIniciadaHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.TRANSMISION_INICIADA;
    }

    @Override
    public Class<TransmisionIniciada> clase() {
        return TransmisionIniciada.class;
    }

    @Override
    public void manejar(EventoEnvelope<TransmisionIniciada> evento) {
        difusor.aSala(evento.datos().subastaId(), "TRANSMISION_INICIADA", evento.datos());
    }
}
