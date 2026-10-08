package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.TiempoExtendido;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-18: aviso de tiempo extendido y nueva hora de fin para el temporizador de la sala. */
@Component
public class TiempoExtendidoHandler implements ManejadorDeEvento<TiempoExtendido> {

    private final Difusor difusor;

    public TiempoExtendidoHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.TIEMPO_EXTENDIDO;
    }

    @Override
    public Class<TiempoExtendido> clase() {
        return TiempoExtendido.class;
    }

    @Override
    public void manejar(EventoEnvelope<TiempoExtendido> evento) {
        difusor.aSala(evento.datos().subastaId(), "TIEMPO_EXTENDIDO", evento.datos());
    }
}
