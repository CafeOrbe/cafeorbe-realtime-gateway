package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.SubastaCerrada;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-19 y HU-21: la sala pasa al estado final y todos los conectados ven el anuncio del ganador o de subasta desierta. */
@Component
public class SubastaCerradaHandler implements ManejadorDeEvento<SubastaCerrada> {

    private final Difusor difusor;

    public SubastaCerradaHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.SUBASTA_CERRADA;
    }

    @Override
    public Class<SubastaCerrada> clase() {
        return SubastaCerrada.class;
    }

    @Override
    public void manejar(EventoEnvelope<SubastaCerrada> evento) {
        difusor.aSala(evento.datos().subastaId(), "SUBASTA_CERRADA", evento.datos());
    }
}
