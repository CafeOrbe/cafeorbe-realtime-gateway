package com.cafeorbe.realtime;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.Rol;
import com.cafeorbe.contracts.eventos.PujaAceptada;
import com.cafeorbe.contracts.eventos.PujaRechazada;
import com.cafeorbe.contracts.eventos.SubastaIniciada;
import com.cafeorbe.contracts.eventos.TransmisionDetenida;
import com.cafeorbe.contracts.eventos.TransmisionIniciada;
import com.cafeorbe.realtime.client.AuctionClient;
import com.cafeorbe.realtime.handlers.EventosDispatcher;
import com.cafeorbe.realtime.ws.Identidad;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class SalaWebSocketTest {

    static final String SECRETO = "cafeorbe-dev-jwt-secret-cambiar-en-prod-0123456789";
    static final UUID SUBASTA = UUID.randomUUID();
    static final UUID OTRA_SUBASTA = UUID.randomUUID();
    static final UUID ANA = UUID.randomUUID();
    static final UUID BRUNO = UUID.randomUUID();
    static final UUID LUIS = UUID.randomUUID();

    @LocalServerPort int puerto;
    @Autowired EventosDispatcher dispatcher;
    @Autowired ObjectMapper json;
    @MockitoBean AuctionClient auction;

    private final List<Cliente> abiertos = new ArrayList<>();

    /** Cliente WebSocket de prueba que acumula los mensajes recibidos. */
    static class Cliente {
        final BlockingQueue<JsonNode> recibidos = new LinkedBlockingQueue<>();
        WebSocket ws;

        JsonNode siguiente(String tipo) throws InterruptedException {
            long limite = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < limite) {
                JsonNode m = recibidos.poll(200, TimeUnit.MILLISECONDS);
                if (m != null && tipo.equals(m.get("tipo").asText())) {
                    return m;
                }
            }
            throw new AssertionError("No llegó un mensaje " + tipo + " a tiempo");
        }

        void noDebeRecibir(String tipo) throws InterruptedException {
            Thread.sleep(400);
            for (JsonNode m : recibidos) {
                assertThat(m.get("tipo").asText()).isNotEqualTo(tipo);
            }
        }

        void enviar(String texto) {
            ws.sendText(texto, true).join();
        }

        void cerrar() {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "adiós").join();
        }
    }

    private String token(UUID id, String nombre, Rol rol) {
        return Jwts.builder().subject(id.toString()).claim("nombre", nombre).claim("rol", rol.name())
                .expiration(Date.from(Instant.now().plusSeconds(600)))
                .signWith(Keys.hmacShaKeyFor(SECRETO.getBytes(StandardCharsets.UTF_8))).compact();
    }

    private Cliente conectar(UUID subasta, String token) throws Exception {
        Cliente cliente = new Cliente();
        var uri = URI.create("ws://localhost:" + puerto + "/ws/salas/" + subasta + "?token=" + token);
        cliente.ws = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(uri, new WebSocket.Listener() {
            private final StringBuilder parcial = new StringBuilder();

            @Override
            public void onOpen(WebSocket ws) {
                ws.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence datos, boolean ultimo) {
                parcial.append(datos);
                if (ultimo) {
                    try {
                        cliente.recibidos.add(json.readTree(parcial.toString()));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    parcial.setLength(0);
                }
                ws.request(1);
                return null;
            }
        }).get(5, TimeUnit.SECONDS);
        abiertos.add(cliente);
        return cliente;
    }

    private Cliente comoAna(UUID subasta) throws Exception {
        return conectar(subasta, token(ANA, "Ana", Rol.COMPRADOR));
    }

    private Cliente comoBruno(UUID subasta) throws Exception {
        return conectar(subasta, token(BRUNO, "Bruno", Rol.COMPRADOR));
    }

    @AfterEach
    void cerrarTodo() {
        abiertos.forEach(c -> {
            try {
                c.ws.abort();
            } catch (RuntimeException ignorada) {
                // ya estaba cerrado
            }
        });
        abiertos.clear();
    }

    private void evento(String tipo, Object datos) throws Exception {
        var sobre = new EventoEnvelope<>(UUID.randomUUID(), tipo, 1, Instant.now(), datos);
        dispatcher.despachar(tipo, json.writeValueAsBytes(sobre));
    }

    // ── Conexión y presencia (HU-05, HU-15) ────────────────────────────────

    @Test
    @DisplayName("Sin token o con token inválido la conexión se rechaza")
    void handshakeSinToken() {
        assertThatThrownBy(() -> conectar(SUBASTA, "")).isInstanceOf(ExecutionException.class);
        assertThatThrownBy(() -> conectar(SUBASTA, "no-es-un-jwt")).isInstanceOf(ExecutionException.class);
    }

    @Test
    @DisplayName("HU-05 · Al entrar a la sala el usuario queda registrado como conectado")
    void entradaRegistraPresencia() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        assertThat(ana.siguiente("CONECTADOS").get("datos").get("conectados").asInt()).isEqualTo(1);
    }

    @Test
    @DisplayName("HU-05 y HU-15 · El conteo sube al entrar otro usuario y baja al salir")
    void conteoDeConectados() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        ana.siguiente("CONECTADOS");

        Cliente bruno = comoBruno(SUBASTA);
        assertThat(ana.siguiente("CONECTADOS").get("datos").get("conectados").asInt()).isEqualTo(2);

        bruno.cerrar();
        assertThat(ana.siguiente("CONECTADOS").get("datos").get("conectados").asInt()).isEqualTo(1);
    }

    @Test
    @DisplayName("HU-15 · Dos pestañas del mismo usuario cuentan como una sola persona")
    void mismoUsuarioNoSeDuplica() throws Exception {
        Cliente pestana1 = comoAna(SUBASTA);
        pestana1.siguiente("CONECTADOS");
        Cliente pestana2 = comoAna(SUBASTA);

        assertThat(pestana2.siguiente("CONECTADOS").get("datos").get("conectados").asInt()).isEqualTo(1);
    }

    // ── Difusión de eventos (HU-13, HU-16, HU-11, HU-12) ──────────────────

    @Test
    @DisplayName("HU-13 y HU-16 · PujaAceptada llega a todos los de la sala y no a otras salas")
    void pujaAceptadaSeDifundeALaSala() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        Cliente bruno = comoBruno(SUBASTA);
        Cliente enOtraSala = comoBruno(OTRA_SUBASTA);

        evento(Eventos.PUJA_ACEPTADA, new PujaAceptada(SUBASTA, UUID.randomUUID(), ANA, "Ana", 110, 1, 120, Instant.now()));

        for (Cliente c : List.of(ana, bruno)) {
            JsonNode m = c.siguiente("PUJA_ACEPTADA");
            assertThat(m.get("subastaId").asText()).isEqualTo(SUBASTA.toString());
            assertThat(m.get("datos").get("monto").asLong()).isEqualTo(110);
            assertThat(m.get("datos").get("usuarioNombre").asText()).isEqualTo("Ana");
            assertThat(m.get("datos").get("siguienteMinimo").asLong()).isEqualTo(120);
        }
        enOtraSala.noDebeRecibir("PUJA_ACEPTADA");
    }

    @Test
    @DisplayName("HU-14 · PujaRechazada llega solo a quien pujó")
    void pujaRechazadaSoloAlQuePujo() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        Cliente bruno = comoBruno(SUBASTA);

        evento(Eventos.PUJA_RECHAZADA, new PujaRechazada(SUBASTA, BRUNO, 110, "SALDO_INSUFICIENTE", "Orbes insuficientes"));

        JsonNode m = bruno.siguiente("PUJA_RECHAZADA");
        assertThat(m.get("datos").get("mensaje").asText()).isEqualTo("Orbes insuficientes");
        assertThat(m.get("datos").get("motivo").asText()).isEqualTo("SALDO_INSUFICIENTE");
        ana.noDebeRecibir("PUJA_RECHAZADA");
    }

    @Test
    @DisplayName("HU-12 y HU-11 · SubastaIniciada y los eventos de transmisión se difunden a la sala")
    void subastaYTransmision() throws Exception {
        Cliente ana = comoAna(SUBASTA);

        evento(Eventos.SUBASTA_INICIADA, new SubastaIniciada(SUBASTA, "Lote", 100, 10, 10, Instant.now(), Instant.now().plusSeconds(600)));
        evento(Eventos.TRANSMISION_INICIADA, new TransmisionIniciada(SUBASTA, "subasta-" + SUBASTA));
        evento(Eventos.TRANSMISION_DETENIDA, new TransmisionDetenida(SUBASTA));

        assertThat(ana.siguiente("SUBASTA_INICIADA").get("datos").get("precioBase").asLong()).isEqualTo(100);
        assertThat(ana.siguiente("TRANSMISION_INICIADA").get("datos").get("sala").asText()).isEqualTo("subasta-" + SUBASTA);
        ana.siguiente("TRANSMISION_DETENIDA");
    }

    @Test
    @DisplayName("Un evento desconocido o ilegible se ignora sin romper la sala")
    void eventoDesconocido() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        dispatcher.despachar("evento.inventado", "{}".getBytes(StandardCharsets.UTF_8));
        dispatcher.despachar(Eventos.PUJA_ACEPTADA, "no es json".getBytes(StandardCharsets.UTF_8));

        evento(Eventos.PUJA_ACEPTADA, new PujaAceptada(SUBASTA, UUID.randomUUID(), ANA, "Ana", 110, 1, 120, Instant.now()));
        ana.siguiente("PUJA_ACEPTADA");
    }

    // ── Puja desde el cliente (HU-13) ─────────────────────────────────────

    @Test
    @DisplayName("HU-13 · PUJAR se reenvía a auction con la identidad del token y el monto")
    void pujaSeReenvia() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":110}");

        verify(auction, timeout(3000)).pujar(eq(SUBASTA), eq(new Identidad(ANA, "Ana", Rol.COMPRADOR)), eq(110L));
    }

    @Test
    @DisplayName("HU-13 · Si auction falla, quien pujó recibe un ERROR con el motivo")
    void fallaDeAuction() throws Exception {
        doThrow(new AuctionClient.PujaNoEnviadaException("La subasta no existe"))
                .when(auction).pujar(any(UUID.class), any(Identidad.class), any(Long.class));
        Cliente ana = comoAna(SUBASTA);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":110}");

        assertThat(ana.siguiente("ERROR").get("datos").get("mensaje").asText()).isEqualTo("La subasta no existe");
    }

    @Test
    @DisplayName("Un Subastador no puede pujar y un mensaje inválido devuelve ERROR; PING responde PONG")
    void mensajesInvalidos() throws Exception {
        Cliente luis = conectar(SUBASTA, token(LUIS, "Luis", Rol.SUBASTADOR));
        luis.siguiente("CONECTADOS");

        luis.enviar("{\"tipo\":\"PUJAR\",\"monto\":110}");
        assertThat(luis.siguiente("ERROR").get("datos").get("mensaje").asText()).isEqualTo("No autorizado");

        luis.enviar("esto no es json");
        assertThat(luis.siguiente("ERROR").get("datos").get("mensaje").asText()).isEqualTo("Mensaje no válido");

        luis.enviar("{\"tipo\":\"PING\"}");
        luis.siguiente("PONG");
        assertThat(Duration.ofSeconds(1)).isPositive();
    }
}
