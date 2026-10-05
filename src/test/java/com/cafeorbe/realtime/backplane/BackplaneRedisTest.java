package com.cafeorbe.realtime.backplane;

import com.cafeorbe.realtime.ws.Salas;
import com.cafeorbe.realtime.ws.Sobre;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * El backplane es el que sincroniza las instancias de realtime entre sí. Es la implementación por defecto
 * en producción (modo=redis) y sus dos fallos se registran sin propagar: si se rompieran en silencio, la
 * difusión entre instancias moriría sin que nadie se entere.
 */
class BackplaneRedisTest {

    static final UUID SALA = UUID.randomUUID();
    static final UUID ANA = UUID.randomUUID();

    private final ObjectMapper json = new ObjectMapper();
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final BackplaneRedis backplane = new BackplaneRedis(redis, json);

    private DefaultMessage mensaje(String cuerpo) {
        return new DefaultMessage(new byte[0], cuerpo.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("publicar(): el sobre viaja serializado por el canal compartido")
    void publicaElSobreSerializado() throws Exception {
        var sobre = new Sobre(SALA, ANA, "puja-aceptada", json.valueToTree(Map.of("monto", 500)));

        backplane.publicar(sobre);

        verify(redis).convertAndSend(eq(BackplaneRedis.CANAL), eq(json.writeValueAsString(sobre)));
    }

    @Test
    @DisplayName("publicar(): un sobre no serializable no propaga la excepción ni publica a medias")
    void sobreNoSerializableNoPropaga() throws JsonProcessingException {
        var jsonRoto = mock(ObjectMapper.class);
        when(jsonRoto.writeValueAsString(any()))
                .thenThrow(new JsonProcessingException("no serializable") { });
        var backplaneRoto = new BackplaneRedis(redis, jsonRoto);

        assertThatCode(() -> backplaneRoto.publicar(new Sobre(SALA, ANA, "puja-aceptada", null)))
                .doesNotThrowAnyException();

        verify(redis, never()).convertAndSend(anyString(), anyString());
    }

    @Test
    @DisplayName("receptor(): un mensaje válido se entrega a las salas de esta instancia")
    void entregaElMensajeRecibido() throws Exception {
        var salas = mock(Salas.class);
        var sobre = new Sobre(SALA, ANA, "puja-aceptada", json.valueToTree(Map.of("monto", 500)));

        backplane.receptor(salas).onMessage(mensaje(json.writeValueAsString(sobre)), new byte[0]);

        var captado = ArgumentCaptor.forClass(Sobre.class);
        verify(salas).entregar(captado.capture());
        assertThat(captado.getValue().subastaId()).isEqualTo(SALA);
        assertThat(captado.getValue().usuarioId()).isEqualTo(ANA);
        assertThat(captado.getValue().tipo()).isEqualTo("puja-aceptada");
        assertThat(captado.getValue().datos().get("monto").asInt()).isEqualTo(500);
    }

    @Test
    @DisplayName("receptor(): un mensaje ilegible se descarta sin tumbar el listener")
    void mensajeIlegibleSeDescarta() {
        var salas = mock(Salas.class);

        assertThatCode(() -> backplane.receptor(salas).onMessage(mensaje("esto no es un sobre"), new byte[0]))
                .doesNotThrowAnyException();

        verify(salas, never()).entregar(any());
    }

    @Test
    @DisplayName("contenedorRedis(): el contenedor queda enlazado a la conexión que recibe el canal")
    void elContenedorUsaLaConexionConfigurada() {
        var conexiones = mock(RedisConnectionFactory.class);

        var contenedor = backplane.contenedorRedis(conexiones, mock(Salas.class));

        assertThat(contenedor.getConnectionFactory()).isSameAs(conexiones);
    }

    @Test
    @DisplayName("receptor(): un sobre de otra versión sin un campo obligatorio tampoco lo tumba")
    void sobreIncompletoSeDescarta() {
        var salas = mock(Salas.class);

        assertThatCode(() -> backplane.receptor(salas).onMessage(mensaje("{\"tipo\":\"puja-aceptada\"}"), new byte[0]))
                .doesNotThrowAnyException();

        verify(salas, never()).entregar(any());
    }

    @Test
    @DisplayName("receptor(): un sobre sin tipo se descarta, no llega a Salas con un tipo inventado")
    void sobreSinTipoSeDescarta() {
        var salas = mock(Salas.class);
        var cuerpo = "{\"subastaId\":\"" + SALA + "\"}";

        assertThatCode(() -> backplane.receptor(salas)
                .onMessage(mensaje(cuerpo), new byte[0])).doesNotThrowAnyException();

        verify(salas, never()).entregar(any());
    }
}
