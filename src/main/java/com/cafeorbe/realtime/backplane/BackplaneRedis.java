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
        contenedor.addMessageListener((mensaje, patron) -> {
            try {
                salas.entregar(json.readValue(new String(mensaje.getBody(), StandardCharsets.UTF_8), Sobre.class));
            } catch (IOException e) {
                log.error("Mensaje ilegible en el backplane", e);
            }
        }, new ChannelTopic(CANAL));
        return contenedor;
    }
}
