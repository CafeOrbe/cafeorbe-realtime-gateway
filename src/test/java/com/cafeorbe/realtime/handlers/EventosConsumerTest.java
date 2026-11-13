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

        verify(dispatcher).despachar("puja.aceptada", cuerpo, null, null);
    }

    @Test
    @DisplayName("alRecibir(): cada routing key se propaga sin mezclarla con la de otro evento")
    void noSeMezclanLasRoutingKeys() {
        var dispatcher = mock(EventosDispatcher.class);
        var consumer = new EventosConsumer(dispatcher);

        consumer.alRecibir(mensaje("subasta.cerrada", "a"));
        consumer.alRecibir(mensaje("transmision.iniciada", "b"));

        verify(dispatcher).despachar("subasta.cerrada", "a".getBytes(StandardCharsets.UTF_8), null, null);
        verify(dispatcher).despachar("transmision.iniciada", "b".getBytes(StandardCharsets.UTF_8), null, null);
    }

    @Test
    @DisplayName("REN-01/DIS-01 · La cola es durable y de consumidor activo único: con 2 réplicas los eventos no se desordenan")
    void colaDeConsumidorActivoUnico() {
        var cola = new EventosConsumer(mock(EventosDispatcher.class)).colaDeEventos();

        org.assertj.core.api.Assertions.assertThat(cola.getName()).isEqualTo("realtime.eventos.ordenados");
        org.assertj.core.api.Assertions.assertThat(cola.isDurable()).isTrue();
        org.assertj.core.api.Assertions.assertThat(cola.getArguments()).containsEntry("x-single-active-consumer", true);
    }

    private Message mensaje(String routingKey, String cuerpo) {
        var props = new MessageProperties();
        props.setReceivedRoutingKey(routingKey);
        return new Message(cuerpo.getBytes(StandardCharsets.UTF_8), props);
    }

    @org.junit.jupiter.api.Test
    @DisplayName("REN-01 · La cabecera x-secuencia se lee como número aunque llegue como texto; lo inválido es nulo")
    void secuencia() {
        org.assertj.core.api.Assertions.assertThat(EventosConsumer.secuencia(42)).isEqualTo(42L);
        org.assertj.core.api.Assertions.assertThat(EventosConsumer.secuencia(42L)).isEqualTo(42L);
        org.assertj.core.api.Assertions.assertThat(EventosConsumer.secuencia("7")).isEqualTo(7L);
        org.assertj.core.api.Assertions.assertThat(EventosConsumer.secuencia("x")).isNull();
        org.assertj.core.api.Assertions.assertThat(EventosConsumer.secuencia(null)).isNull();

        var dispatcher = mock(EventosDispatcher.class);
        var props = new MessageProperties();
        props.setReceivedRoutingKey("puja.aceptada");
        props.setHeader("x-secuencia", 9L);
        props.setHeader("x-correlation-id", "corr-1");
        byte[] cuerpo = "{}".getBytes(StandardCharsets.UTF_8);
        new EventosConsumer(dispatcher).alRecibir(new Message(cuerpo, props));
        verify(dispatcher).despachar("puja.aceptada", cuerpo, 9L, "corr-1");
    }
}
