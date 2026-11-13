package com.cafeorbe.realtime.ws;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
public class ConfiguracionWebSocket implements WebSocketConfigurer {

    /**
     * SEG-03: el contenedor corta por sí mismo (cierre 1009) los mensajes de más de 8 KB. Entre 1 KB y 8 KB el
     * mensaje llega al manejador, que lo rechaza y cuenta la infracción.
     */
    static final int BUFFER_MAXIMO = 8 * 1024;

    private final SalaWebSocketHandler manejador;
    private final HandshakeConTicket handshake;
    private final String[] origenes;

    public ConfiguracionWebSocket(SalaWebSocketHandler manejador, HandshakeConTicket handshake,
                                  @Value("${cafeorbe.cors.origins}") String[] origenes) {
        this.manejador = manejador;
        this.handshake = handshake;
        this.origenes = origenes;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registro) {
        registro.addHandler(manejador, "/ws/salas/*")
                .addInterceptors(handshake)
                .setAllowedOriginPatterns(origenes);
    }

    @Bean
    ServletServerContainerFactoryBean contenedorWebSocket() {
        var contenedor = new ServletServerContainerFactoryBean();
        contenedor.setMaxTextMessageBufferSize(BUFFER_MAXIMO);
        contenedor.setMaxBinaryMessageBufferSize(BUFFER_MAXIMO);
        return contenedor;
    }
}
