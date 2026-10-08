package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.OrbesCobrados;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-20: solo el comprador al que se le cobró recibe el aviso, para refrescar su saldo. */
@Component
public class OrbesCobradosHandler implements ManejadorDeEvento<OrbesCobrados> {

    private final Difusor difusor;

    public OrbesCobradosHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.ORBES_COBRADOS;
    }

    @Override
    public Class<OrbesCobrados> clase() {
        return OrbesCobrados.class;
    }

    @Override
    public void manejar(EventoEnvelope<OrbesCobrados> evento) {
        difusor.aUsuario(evento.datos().subastaId(), evento.datos().usuarioId(), "ORBES_COBRADOS", evento.datos());
    }
}
