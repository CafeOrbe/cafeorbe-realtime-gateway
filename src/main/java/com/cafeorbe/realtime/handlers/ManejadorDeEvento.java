package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.EventoEnvelope;

/** Un manejador por tipo de evento del broker. Recibe el hecho y decide a quién difundirlo. */
public interface ManejadorDeEvento<T> {

    /** Routing key del evento que maneja (ver {@code Eventos}). */
    String tipoDeEvento();

    Class<T> clase();

    void manejar(EventoEnvelope<T> evento);
}
