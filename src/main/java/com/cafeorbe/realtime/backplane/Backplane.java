package com.cafeorbe.realtime.backplane;

import com.cafeorbe.realtime.ws.Sobre;

/** Reparte un mensaje a las conexiones de TODAS las instancias del realtime-gateway. */
public interface Backplane {

    void publicar(Sobre sobre);
}
