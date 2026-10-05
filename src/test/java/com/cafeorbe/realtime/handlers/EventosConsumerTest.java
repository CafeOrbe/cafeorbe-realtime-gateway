package com.cafeorbe.realtime.handlers;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * La cola es única y compartida: este listener es el único punto de entrada de los eventos, así que
 * perder la routing key aquí significaría que un evento se difunde como si fuera de otro tipo.
 */
class EventosConsumerTest {

    @Test
    @DisplayName("alRecibir(): el evento se despacha con la routing key con la que llegó y su cuerpo intacto")
    void despachaConLaRoutingKeyRecibida() {
        var dispatcher = mock(EventosDispatcher.class);
        var cuerpo = "{\"tipo\":\"puja.aceptada\",\"datos\":{\"monto\":500}}".getBytes(StandardCharsets.UTF_8);
        var props = new MessageProperties();
        props.setReceivedRoutingKey("puja.aceptada");

        new EventosConsumer(dispatcher).alRecibir(new Message(cuerpo, props));

        verify(dispatcher).despachar("puja.aceptada", cuerpo);
    }

    @Test
    @DisplayName("alRecibir(): cada routing key se propaga sin mezclarla con la de otro evento")
    void noSeMezclanLasRoutingKeys() {
        var dispatcher = mock(EventosDispatcher.class);
        var consumer = new EventosConsumer(dispatcher);

        consumer.alRecibir(mensaje("subasta.cerrada", "a"));
        consumer.alRecibir(mensaje("transmision.iniciada", "b"));

        verify(dispatcher).despachar("subasta.cerrada", "a".getBytes(StandardCharsets.UTF_8));
        verify(dispatcher).despachar("transmision.iniciada", "b".getBytes(StandardCharsets.UTF_8));
    }

    private Message mensaje(String routingKey, String cuerpo) {
        var props = new MessageProperties();
        props.setReceivedRoutingKey(routingKey);
        return new Message(cuerpo.getBytes(StandardCharsets.UTF_8), props);
    }
}
