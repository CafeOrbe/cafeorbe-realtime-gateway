package com.cafeorbe.realtime.handlers;

import com.cafeorbe.contracts.Eventos;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Cola única compartida por todas las instancias: cada evento lo procesa una sola, que lo difunde por el
 * backplane a las conexiones de todas. Así una sala repartida entre varias instancias ve los mismos mensajes.
 */
@Configuration
public class EventosConsumer {

    static final String COLA = "realtime.eventos";

    private final EventosDispatcher dispatcher;

    public EventosConsumer(EventosDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Bean
    TopicExchange eventosExchange() {
        return new TopicExchange(Eventos.EXCHANGE, true, false);
    }

    @Bean
    Queue colaDeEventos() {
        return QueueBuilder.durable(COLA).build();
    }

    @Bean
    Binding vinculos(Queue colaDeEventos, TopicExchange eventosExchange) {
        // Sprint 2 agregará: subasta.cerrada, tiempo.extendido, orbes.cobrados
        return BindingBuilder.bind(colaDeEventos).to(eventosExchange).with("subasta.*");
    }

    @Bean
    Binding vinculoPujas(Queue colaDeEventos, TopicExchange eventosExchange) {
        return BindingBuilder.bind(colaDeEventos).to(eventosExchange).with("puja.*");
    }

    @Bean
    Binding vinculoTransmision(Queue colaDeEventos, TopicExchange eventosExchange) {
        return BindingBuilder.bind(colaDeEventos).to(eventosExchange).with("transmision.*");
    }

    @RabbitListener(queues = COLA)
    public void alRecibir(Message mensaje) {
        dispatcher.despachar(mensaje.getMessageProperties().getReceivedRoutingKey(), mensaje.getBody());
    }
}
