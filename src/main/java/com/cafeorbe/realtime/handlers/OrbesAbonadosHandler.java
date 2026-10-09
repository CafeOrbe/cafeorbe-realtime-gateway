package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.eventos.OrbesAbonados;
import com.cafeorbe.realtime.backplane.Difusor;
import org.springframework.stereotype.Component;

/** HU-24: solo el Subastador al que se le abonó recibe el aviso, para refrescar su saldo. */
@Component
public class OrbesAbonadosHandler implements ManejadorDeEvento<OrbesAbonados> {

    private final Difusor difusor;

    public OrbesAbonadosHandler(Difusor difusor) {
        this.difusor = difusor;
    }

    @Override
    public String tipoDeEvento() {
        return Eventos.ORBES_ABONADOS;
    }

    @Override
    public Class<OrbesAbonados> clase() {
        return OrbesAbonados.class;
    }

    @Override
    public void manejar(EventoEnvelope<OrbesAbonados> evento) {
        difusor.aUsuario(evento.datos().subastaId(), evento.datos().usuarioId(), "ORBES_ABONADOS", evento.datos());
    }
}
