package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.TransmisionDetenida;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-11: los compradores ven el mensaje Transmisión finalizada. */
@Component
public class TransmisionDetenidaHandler implements ManejadorDeEvento<TransmisionDetenida> {

    private final Difusor difusor;

    public TransmisionDetenidaHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.TRANSMISION_DETENIDA;
    }

    @Override
    public Class<TransmisionDetenida> clase() {
        return TransmisionDetenida.class;
    }

    @Override
    public void manejar(EventoEnvelope<TransmisionDetenida> evento) {
        difusor.aSala(evento.datos().subastaId(), "TRANSMISION_DETENIDA", evento.datos());
    }
}
