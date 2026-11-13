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
 *
 * <p>REN-01 y DIS-01: la cola es de <em>consumidor activo único</em>. Con dos réplicas compitiendo por la misma
 * cola, dos eventos seguidos podían procesarse en paralelo y llegar a la sala en desorden, y el navegador
 * descarta lo que llega con una secuencia menor a la última vista: se perdía una puja aceptada. Así solo una
 * réplica consume a la vez, en el orden de la cola; si esa réplica cae, RabbitMQ pasa el consumo a otra y
 * reentrega lo no confirmado (el navegador descarta los repetidos por secuencia).
 *
 * <p>El nombre cambió respecto al Sprint 1 ({@code realtime.eventos}) porque RabbitMQ no permite cambiar los
 * argumentos de una cola ya declarada. La cola vieja hay que borrarla a mano tras el despliegue.
 */
@Configuration
public class EventosConsumer {

    static final String COLA = "realtime.eventos.ordenados";

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
        return QueueBuilder.durable(COLA).singleActiveConsumer().build();
    }

    @Bean
    Binding vinculos(Queue colaDeEventos, TopicExchange eventosExchange) {
        // subasta.iniciada y subasta.cerrada
        return BindingBuilder.bind(colaDeEventos).to(eventosExchange).with("subasta.*");
    }

    /** HU-18: tiempo.extendido */
    @Bean
    Binding vinculoTiempo(Queue colaDeEventos, TopicExchange eventosExchange) {
        return BindingBuilder.bind(colaDeEventos).to(eventosExchange).with("tiempo.*");
    }

    /** HU-20: orbes.cobrados */
    @Bean
    Binding vinculoOrbes(Queue colaDeEventos, TopicExchange eventosExchange) {
        return BindingBuilder.bind(colaDeEventos).to(eventosExchange).with("orbes.*");
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
        var propiedades = mensaje.getMessageProperties();
        dispatcher.despachar(propiedades.getReceivedRoutingKey(), mensaje.getBody(),
                secuencia(propiedades.getHeader(CABECERA_SECUENCIA)), propiedades.getHeader(CABECERA_CORRELACION));
    }

    /** REN-01: auction numera sus eventos con el id de su outbox. */
    static final String CABECERA_SECUENCIA = "x-secuencia";
    /** OBS-01: correlationId de la puja que originó el evento. */
    static final String CABECERA_CORRELACION = "x-correlation-id";

    static Long secuencia(Object valor) {
        if (valor instanceof Number n) {
            return n.longValue();
        }
        if (valor instanceof String texto) {
            try {
                return Long.parseLong(texto);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
}
