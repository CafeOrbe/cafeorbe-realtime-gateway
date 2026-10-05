package com.cafeorbe.realtime.backplane;

import com.cafeorbe.realtime.ws.Salas;
import com.cafeorbe.realtime.ws.Sobre;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Backplane pub/sub sobre Redis: cada instancia publica los mensajes en un canal y todas
 * (incluida la propia) los reciben y los entregan a sus conexiones locales.
 */
@Configuration
@ConditionalOnProperty(name = "cafeorbe.realtime.modo", havingValue = "redis", matchIfMissing = true)
public class BackplaneRedis implements Backplane {

    static final String CANAL = "cafeorbe.salas";
    private static final Logger log = LoggerFactory.getLogger(BackplaneRedis.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public BackplaneRedis(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    @Override
    public void publicar(Sobre sobre) {
        try {
            redis.convertAndSend(CANAL, json.writeValueAsString(sobre));
        } catch (JsonProcessingException e) {
            log.error("No se pudo serializar el sobre {}", sobre.tipo(), e);
        }
    }

    @Bean
    RedisMessageListenerContainer contenedorRedis(RedisConnectionFactory conexiones, Salas salas) {
        var contenedor = new RedisMessageListenerContainer();
        contenedor.setConnectionFactory(conexiones);
        contenedor.addMessageListener(receptor(salas), new ChannelTopic(CANAL));
        return contenedor;
    }

    /**
     * Extraído del @Bean solo para poder probar el mensaje ilegible, que se traga la IOException y solo la
     * registra. Inlinearlo de vuelta no cambia el comportamiento, pero deja ese camino sin verificar.
     */
    MessageListener receptor(Salas salas) {
        return (mensaje, patron) -> {
            String cuerpo = new String(mensaje.getBody(), StandardCharsets.UTF_8);
            Sobre sobre;
            try {
                sobre = json.readValue(cuerpo, Sobre.class);
            } catch (IOException e) {
                log.error("Mensaje ilegible en el backplane", e);
                return;
            }
            // Sin esta guarda, un sobre sin subastaId hace que Salas.entregar haga get(null) sobre su
            // ConcurrentHashMap y reviente con NullPointerException, que el catch de arriba no cubre.
            if (sobre.subastaId() == null || sobre.tipo() == null) {
                log.warn("Se descarta un sobre del backplane sin subastaId o tipo ({} bytes)", cuerpo.length());
                return;
            }
            salas.entregar(sobre);
        };
    }
}
