package com.cafeorbe.realtime.ws;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class ConfiguracionWebSocket implements WebSocketConfigurer {

    private final SalaWebSocketHandler manejador;
    private final HandshakeConToken handshake;
    private final String[] origenes;

    public ConfiguracionWebSocket(SalaWebSocketHandler manejador, HandshakeConToken handshake,
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
}
