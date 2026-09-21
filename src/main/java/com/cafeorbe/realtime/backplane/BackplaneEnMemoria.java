package com.cafeorbe.realtime.backplane;

import com.cafeorbe.realtime.ws.Salas;
import com.cafeorbe.realtime.ws.Sobre;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Para pruebas o una sola instancia: entrega directo a las conexiones locales. */
@Component
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "memoria")
public class BackplaneEnMemoria implements Backplane {

    private final Salas salas;

    public BackplaneEnMemoria(Salas salas) {
        this.salas = salas;
    }

    @Override
    public void publicar(Sobre sobre) {
        salas.entregar(sobre);
    }
}
