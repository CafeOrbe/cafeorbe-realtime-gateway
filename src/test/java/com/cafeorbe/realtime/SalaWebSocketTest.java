package com.cafeorbe.realtime;

import com.cafeorbe.contracts.EventoEnvelope;
import com.cafeorbe.contracts.Eventos;
import com.cafeorbe.contracts.Rol;
import com.cafeorbe.contracts.eventos.OrbesAbonados;
import com.cafeorbe.contracts.eventos.OrbesCobrados;
import com.cafeorbe.contracts.eventos.PujaAceptada;
import com.cafeorbe.contracts.eventos.PujaRechazada;
import com.cafeorbe.contracts.eventos.SubastaCerrada;
import com.cafeorbe.contracts.eventos.SubastaIniciada;
import com.cafeorbe.contracts.eventos.TiempoExtendido;
import com.cafeorbe.contracts.eventos.TransmisionDetenida;
import com.cafeorbe.contracts.eventos.TransmisionIniciada;
import com.cafeorbe.realtime.client.AuctionClient;
import com.cafeorbe.realtime.handlers.EventosDispatcher;
import com.cafeorbe.realtime.seguridad.AlmacenDeSeguridadEnMemoria;
import com.cafeorbe.realtime.ws.Identidad;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability(tracing = false)
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
        final java.util.concurrent.CompletableFuture<Integer> cierre = new java.util.concurrent.CompletableFuture<>();
        WebSocket ws;

        int codigoDeCierre(long segundos) throws Exception {
            return cierre.get(segundos, TimeUnit.SECONDS);
        }

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

    @Autowired AlmacenDeSeguridadEnMemoria almacen;

    /** SEG-04: lo que identity habría emitido; devuelve el ticket de un solo uso. */
    private String token(UUID id, String nombre, Rol rol) {
        return ticket(id, nombre, rol, UUID.randomUUID().toString(), Instant.now().plusSeconds(600));
    }

    private String ticket(UUID id, String nombre, Rol rol, String jti, Instant expiraEn) {
        String ticket = UUID.randomUUID().toString().replace("-", "");
        almacen.guardarTicket(ticket, "{\"usuarioId\":\"" + id + "\",\"nombre\":\"" + nombre + "\",\"rol\":\""
                + rol.name() + "\",\"jti\":\"" + jti + "\",\"expiraEn\":" + expiraEn.getEpochSecond() + "}");
        return ticket;
    }

    private Cliente conectar(UUID subasta, String ticket) throws Exception {
        return abrir("ws://localhost:" + puerto + "/ws/salas/" + subasta + "?ticket=" + ticket);
    }

    private Cliente conectarSinToken(UUID subasta) throws Exception {
        return abrir("ws://localhost:" + puerto + "/ws/salas/" + subasta);
    }

    private Cliente conectarConSegmento(String segmento, String token) throws Exception {
        return abrir("ws://localhost:" + puerto + "/ws/salas/" + segmento + "?ticket=" + token);
    }

    private Cliente abrir(String destino) throws Exception {
        Cliente cliente = new Cliente();
        var uri = URI.create(destino);
        cliente.ws = HttpClient.newHttpClient().newWebSocketBuilder().buildAsync(uri, new WebSocket.Listener() {
            private final StringBuilder parcial = new StringBuilder();

            @Override
            public void onOpen(WebSocket ws) {
                ws.request(1);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket ws, int codigo, String razon) {
                cliente.cierre.complete(codigo);
                return null;
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
        almacen.limpiar();
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
    @DisplayName("HU-05 · Una subasta que no existe no abre sala ni registra presencia")
    void subastaInexistente() throws Exception {
        UUID inexistente = UUID.randomUUID();
        when(auction.noExiste(eq(inexistente), any(Identidad.class))).thenReturn(true);
        assertThatThrownBy(() -> comoAna(inexistente)).isInstanceOf(ExecutionException.class);
        // Una subasta existente (o auction sin responder) sí deja entrar.
        assertThat(comoAna(SUBASTA).siguiente("CONECTADOS").get("datos").get("conectados").asInt()).isEqualTo(1);
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
        var recibido = ana.siguiente("PUJA_ACEPTADA");
        assertThat(recibido.get("datos").get("monto").asLong()).isEqualTo(110);
    }

    // ── Puja desde el cliente (HU-13) ─────────────────────────────────────

    @Test
    @DisplayName("HU-13 · PUJAR se reenvía a auction con la identidad del token y el monto")
    void pujaSeReenvia() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":110}");

        verify(auction, timeout(3000)).pujar(eq(SUBASTA),
                org.mockito.ArgumentMatchers.argThat(i -> i.usuarioId().equals(ANA) && i.rol() == Rol.COMPRADOR),
                eq(110L), any(String.class));
    }

    @Test
    @DisplayName("HU-13 · Si auction falla, quien pujó recibe un ERROR con el motivo")
    void fallaDeAuction() throws Exception {
        doThrow(new AuctionClient.PujaNoEnviadaException("La subasta no existe"))
                .when(auction).pujar(any(UUID.class), any(Identidad.class), any(Long.class), any(String.class));
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

    // ── Sprint 2: extensión, cierre y cobro ───────────────────────────────

    @Test
    @DisplayName("HU-18 · TiempoExtendido llega a toda la sala con la nueva hora de fin")
    void tiempoExtendido() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        Cliente bruno = comoBruno(SUBASTA);
        Instant nuevoFin = Instant.parse("2026-10-05T15:10:30Z");

        evento(Eventos.TIEMPO_EXTENDIDO, new TiempoExtendido(SUBASTA, 30, nuevoFin, 1, 3));

        for (Cliente c : List.of(ana, bruno)) {
            JsonNode m = c.siguiente("TIEMPO_EXTENDIDO");
            assertThat(m.get("datos").get("segundosExtendidos").asInt()).isEqualTo(30);
            assertThat(Instant.parse(m.get("datos").get("horaFin").asText())).isEqualTo(nuevoFin);
        }
    }

    @Test
    @DisplayName("HU-21 · Anuncio con ganador: todos los conectados reciben el nombre del ganador y el monto final")
    void anuncioConGanador() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        Cliente bruno = comoBruno(SUBASTA);
        Cliente enOtraSala = comoBruno(OTRA_SUBASTA);

        evento(Eventos.SUBASTA_CERRADA, new SubastaCerrada(SUBASTA, "Lote", "FINALIZADA", ANA, "Ana", 300L, 5, Instant.now()));

        for (Cliente c : List.of(ana, bruno)) {
            JsonNode m = c.siguiente("SUBASTA_CERRADA");
            assertThat(m.get("datos").get("estado").asText()).isEqualTo("FINALIZADA");
            assertThat(m.get("datos").get("ganadorNombre").asText()).isEqualTo("Ana");
            assertThat(m.get("datos").get("montoFinal").asLong()).isEqualTo(300);
        }
        enOtraSala.noDebeRecibir("SUBASTA_CERRADA");
    }

    @Test
    @DisplayName("HU-21 · Anuncio de subasta desierta: el evento llega a la sala sin ganador")
    void anuncioDesierta() throws Exception {
        Cliente ana = comoAna(SUBASTA);

        evento(Eventos.SUBASTA_CERRADA, new SubastaCerrada(SUBASTA, "Lote", "DESIERTA", null, null, null, 0, Instant.now()));

        JsonNode m = ana.siguiente("SUBASTA_CERRADA");
        assertThat(m.get("datos").get("estado").asText()).isEqualTo("DESIERTA");
        assertThat(m.get("datos").get("ganadorId").isNull()).isTrue();
    }

    @Test
    @DisplayName("HU-20 · OrbesCobrados llega solo al comprador al que se le cobró")
    void orbesCobrados() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        Cliente bruno = comoBruno(SUBASTA);

        evento(Eventos.ORBES_COBRADOS, new OrbesCobrados(SUBASTA, ANA, 300, 700));

        assertThat(ana.siguiente("ORBES_COBRADOS").get("datos").get("saldo").asLong()).isEqualTo(700);
        bruno.noDebeRecibir("ORBES_COBRADOS");
    }

    @Test
    @DisplayName("HU-24 · OrbesAbonados llega solo al Subastador al que se le abonó")
    void orbesAbonados() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        Cliente bruno = comoBruno(SUBASTA);

        evento(Eventos.ORBES_ABONADOS, new OrbesAbonados(SUBASTA, ANA, 300, 1300));

        assertThat(ana.siguiente("ORBES_ABONADOS").get("datos").get("saldo").asLong()).isEqualTo(1300);
        bruno.noDebeRecibir("ORBES_ABONADOS");
    }

    @Test
    @DisplayName("Un tipo de mensaje desconocido responde ERROR y la sala sigue viva")
    void tipoDeMensajeDesconocido() throws Exception {
        UUID subasta = UUID.randomUUID(); // propia: el reenvío corre aparte y otras pruebas pueden seguir pujando
        Cliente ana = comoAna(subasta);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"COBRAR\",\"monto\":9999}");

        assertThat(ana.siguiente("ERROR").get("datos").get("mensaje").asText())
                .isEqualTo("Mensaje no reconocido");
        ana.enviar("{\"tipo\":\"PING\"}");
        ana.siguiente("PONG");
        verify(auction, never()).pujar(eq(subasta), any(Identidad.class), any(Long.class), any());
    }

    @Test
    @DisplayName("Una puja con un monto que no es número se rechaza sin llegar a auction")
    void montoNoNumerico() throws Exception {
        UUID subasta = UUID.randomUUID();
        Cliente ana = comoAna(subasta);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":\"mucho\"}");

        assertThat(ana.siguiente("ERROR").get("datos").get("mensaje").asText())
                .isEqualTo("El monto de la puja no es válido");
        Thread.sleep(300); // da tiempo a un reenvío en segundo plano, si lo hubiera
        verify(auction, never()).pujar(eq(subasta), any(Identidad.class), any(Long.class), any());
    }

    @Test
    @DisplayName("Una puja sin monto se rechaza: la validación de entrada es la misma")
    void pujaSinMonto() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"PUJAR\"}");

        assertThat(ana.siguiente("ERROR").get("datos").get("mensaje").asText())
                .isEqualTo("El monto de la puja no es válido");
    }

    @Test
    @DisplayName("Una ruta de sala que no es UUID se rechaza en el handshake")
    void rutaNoEsUuid() {
        assertThatThrownBy(() -> conectarConSegmento("no-es-uuid", token(ANA, "Ana", Rol.COMPRADOR)))
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    @DisplayName("Una conexión sin el parámetro token se rechaza")
    void sinParametroToken() {
        assertThatThrownBy(() -> conectarSinToken(SUBASTA)).isInstanceOf(ExecutionException.class);
    }

    // ── Seguridad (SEG-03, SEG-04, SEG-05) y orden (REN-01) ───────────────

    @Autowired io.micrometer.core.instrument.MeterRegistry metricas;

    private double eventosDeSeguridad(String evento) {
        return metricas.find("cafeorbe.seguridad.eventos").tag("evento", evento).counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }

    @Test
    @DisplayName("SEG-04 · 10 intentos de reutilizar una URL de conexión ya usada: 0 aceptados y todos registrados")
    void ticketReutilizado() throws Exception {
        String ticket = token(UUID.randomUUID(), "Eva", Rol.COMPRADOR);
        conectar(SUBASTA, ticket);
        double antes = eventosDeSeguridad("conexion_rechazada");
        for (int i = 0; i < 10; i++) {
            assertThatThrownBy(() -> conectar(SUBASTA, ticket)).isInstanceOf(ExecutionException.class);
        }
        assertThat(eventosDeSeguridad("conexion_rechazada") - antes).isEqualTo(10);
    }

    @Test
    @DisplayName("SEG-04 · El token de sesión en la URL ya no sirve para conectarse")
    void tokenEnLaUrlNoSirve() {
        assertThatThrownBy(() -> abrir("ws://localhost:" + puerto + "/ws/salas/" + SUBASTA + "?token=eyJhbGciOi.x.y"))
                .isInstanceOf(ExecutionException.class);
    }

    @Test
    @DisplayName("SEG-05 · Un ticket de una sesión ya cerrada o vencida no abre conexión")
    void ticketDeSesionCerradaOVencida() {
        String jti = UUID.randomUUID().toString();
        almacen.cerrarSesion(jti);
        String deCerrada = ticket(UUID.randomUUID(), "Eva", Rol.COMPRADOR, jti, Instant.now().plusSeconds(600));
        String deVencida = ticket(UUID.randomUUID(), "Eva", Rol.COMPRADOR, "otra", Instant.now().minusSeconds(1));
        assertThatThrownBy(() -> conectar(SUBASTA, deCerrada)).isInstanceOf(ExecutionException.class);
        assertThatThrownBy(() -> conectar(SUBASTA, deVencida)).isInstanceOf(ExecutionException.class);
    }

    @Test
    @DisplayName("SEG-05 · Al cerrar la sesión, su WebSocket abierto se corta en 5 s o menos (cierre 4401)")
    void sesionCerradaCortaElWebSocket() throws Exception {
        String jti = UUID.randomUUID().toString();
        Cliente eva = conectar(SUBASTA, ticket(UUID.randomUUID(), "Eva", Rol.COMPRADOR, jti, Instant.now().plusSeconds(600)));
        long inicio = System.nanoTime();
        almacen.cerrarSesion(jti);
        assertThat(eva.codigoDeCierre(5)).isEqualTo(4401);
        assertThat(Duration.ofNanos(System.nanoTime() - inicio)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("SEG-05 · Cuando la sesión vence, su WebSocket se corta en 5 s o menos")
    void sesionVencidaCortaElWebSocket() throws Exception {
        Cliente eva = conectar(SUBASTA, ticket(UUID.randomUUID(), "Eva", Rol.COMPRADOR, "j-vence",
                Instant.now().plusSeconds(2)));
        assertThat(eva.codigoDeCierre(7)).isEqualTo(4401);
    }

    @Test
    @DisplayName("SEG-03 · Ráfaga de 30 PUJAR: el excedente recibe LIMITE_DE_PUJAS, el abusador queda bloqueado y los demás siguen")
    void rafagaDePujas() throws Exception {
        UUID eva = UUID.randomUUID();
        Cliente cliente = conectar(SUBASTA, token(eva, "Eva", Rol.COMPRADOR));
        for (int i = 0; i < 30; i++) {
            cliente.enviar("{\"tipo\":\"PUJAR\",\"monto\":" + (100 + i) + "}");
        }
        JsonNode error = cliente.siguiente("ERROR");
        assertThat(error.at("/datos/motivo").asText()).isEqualTo("LIMITE_DE_PUJAS");
        // Tras 3 infracciones en un minuto queda bloqueado: la conexión se corta con 4429.
        assertThat(cliente.codigoDeCierre(5)).isEqualTo(4429);
        verify(auction, org.mockito.Mockito.atMost(20)).pujar(eq(SUBASTA),
                org.mockito.ArgumentMatchers.argThat(i -> i.usuarioId().equals(eva)), any(Long.class), any());
        // Bloqueado: tampoco puede abrir una conexión nueva durante el bloqueo.
        assertThatThrownBy(() -> conectar(SUBASTA, token(eva, "Eva", Rol.COMPRADOR))).isInstanceOf(ExecutionException.class);
        // Un comprador legítimo sigue pujando sin problema.
        Cliente bruno = comoBruno(SUBASTA);
        bruno.enviar("{\"tipo\":\"PUJAR\",\"monto\":500}");
        verify(auction, timeout(3000)).pujar(eq(SUBASTA),
                org.mockito.ArgumentMatchers.argThat(i -> i.usuarioId().equals(BRUNO)), eq(500L), any());
    }

    @Test
    @DisplayName("SEG-03 · Con auction lento (300 ms por puja, como en QA) la ráfaga igual se corta: el límite cuenta al llegar")
    void rafagaConAuctionLento() throws Exception {
        when(auction.pujar(eq(SUBASTA), any(Identidad.class), any(Long.class), any())).thenAnswer(invocacion -> {
            Thread.sleep(300);
            return java.util.Optional.empty();
        });
        UUID eva = UUID.randomUUID();
        Cliente cliente = conectar(SUBASTA, token(eva, "Eva", Rol.COMPRADOR));
        long inicio = System.nanoTime();
        for (int i = 0; i < 30 && !cliente.cierre.isDone(); i++) {
            try {
                cliente.ws.sendText("{\"tipo\":\"PUJAR\",\"monto\":" + (100 + i) + "}", true).join();
            } catch (java.util.concurrent.CompletionException cortada) {
                break; // el servidor ya cerró la conexión: eso es lo que se espera
            }
            Thread.sleep(33);
        }
        assertThat(cliente.siguiente("ERROR").at("/datos/motivo").asText()).isEqualTo("LIMITE_DE_PUJAS");
        assertThat(cliente.codigoDeCierre(5)).isEqualTo(4429);
        // El corte llega mientras dura la ráfaga, no cuando auction termina de procesar la cola.
        assertThat(Duration.ofNanos(System.nanoTime() - inicio)).isLessThan(Duration.ofSeconds(3));
        // Lo que quedó en cola tras el bloqueo no llega a auction: como mucho las ~10 que el límite dejó pasar.
        Thread.sleep(3500);
        verify(auction, org.mockito.Mockito.atMost(12)).pujar(eq(SUBASTA),
                org.mockito.ArgumentMatchers.argThat(i -> i.usuarioId().equals(eva)), any(Long.class), any());
    }

    @Test
    @DisplayName("HU-13 · Un error inesperado al reenviar avisa a quien pujó y no traba sus pujas siguientes")
    void errorInesperadoNoTrabaLaCola() throws Exception {
        UUID subasta = UUID.randomUUID();
        when(auction.pujar(eq(subasta), any(Identidad.class), eq(110L), any())).thenThrow(new IllegalStateException("caído"));
        Cliente ana = comoAna(subasta);
        ana.siguiente("CONECTADOS");

        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":110}");
        assertThat(ana.siguiente("ERROR").at("/datos/mensaje").asText()).isEqualTo("No se pudo registrar la puja, intenta de nuevo");

        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":120}");
        verify(auction, timeout(3000)).pujar(eq(subasta), any(Identidad.class), eq(120L), any());
    }

    @Test
    @DisplayName("HU-13 · Las pujas de una misma conexión llegan a auction en el orden en que se enviaron")
    void pujasEnOrden() throws Exception {
        List<Long> recibidas = java.util.Collections.synchronizedList(new ArrayList<>());
        UUID subasta = UUID.randomUUID();
        when(auction.pujar(eq(subasta), any(Identidad.class), any(Long.class), any())).thenAnswer(invocacion -> {
            long monto = invocacion.getArgument(2);
            Thread.sleep(monto == 110 ? 200 : 10); // la primera tarda más: si se adelantara otra, el orden cambiaría
            recibidas.add(monto);
            return java.util.Optional.empty();
        });
        Cliente ana = comoAna(subasta);
        ana.siguiente("CONECTADOS");
        for (long monto : new long[] {110, 120, 130}) {
            ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":" + monto + "}");
        }
        // Se espera a la lista, no a verify: Mockito anota la llamada al entrar y el monto se agrega al salir.
        long limite = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (recibidas.size() < 3 && System.nanoTime() < limite) {
            Thread.sleep(20);
        }
        assertThat(recibidas).containsExactly(110L, 120L, 130L);
    }

    @Test
    @DisplayName("SEG-03 · La sexta conexión simultánea del mismo usuario se rechaza")
    void maximoDeConexiones() throws Exception {
        UUID eva = UUID.randomUUID();
        for (int i = 0; i < 5; i++) {
            conectar(SUBASTA, token(eva, "Eva", Rol.COMPRADOR));
        }
        assertThatThrownBy(() -> conectar(SUBASTA, token(eva, "Eva", Rol.COMPRADOR))).isInstanceOf(ExecutionException.class);
    }

    @Test
    @DisplayName("SEG-03 · Un mensaje de más de 1 KB cierra la conexión (1009) sin llegar a auction")
    void mensajeGrande() throws Exception {
        UUID subasta = UUID.randomUUID();
        Cliente cliente = conectar(subasta, token(UUID.randomUUID(), "Eva", Rol.COMPRADOR));
        cliente.enviar("{\"tipo\":\"PUJAR\",\"monto\":100,\"relleno\":\"" + "x".repeat(2000) + "\"}");
        assertThat(cliente.codigoDeCierre(5)).isEqualTo(1009);
        Thread.sleep(300);
        verify(auction, never()).pujar(eq(subasta), any(Identidad.class), any(Long.class), any());
    }

    @Test
    @DisplayName("REN-01 · Una puja que auction rechaza sin evento se le entrega directamente a quien pujó")
    void rechazoSinEvento() throws Exception {
        when(auction.pujar(eq(SUBASTA), any(Identidad.class), eq(10L), any())).thenReturn(java.util.Optional.of(
                json.readTree("{\"aceptada\":false,\"monto\":10,\"motivo\":\"MONTO_INSUFICIENTE\",\"mensaje\":\"Muy bajo\",\"porEvento\":false}")));
        Cliente ana = comoAna(SUBASTA);
        ana.enviar("{\"tipo\":\"PUJAR\",\"monto\":10}");
        JsonNode rechazo = ana.siguiente("PUJA_RECHAZADA");
        assertThat(rechazo.at("/datos/motivo").asText()).isEqualTo("MONTO_INSUFICIENTE");
        assertThat(rechazo.at("/datos/montoIntentado").asLong()).isEqualTo(10);
    }

    @Test
    @DisplayName("REN-01 · Los mensajes de un evento de auction llevan su número de secuencia")
    void secuencia() throws Exception {
        Cliente ana = comoAna(SUBASTA);
        var sobre = new EventoEnvelope<>(UUID.randomUUID(), Eventos.PUJA_ACEPTADA, 1, Instant.now(),
                new PujaAceptada(SUBASTA, UUID.randomUUID(), BRUNO, "Bruno", 700, 3, 710, Instant.now()));
        dispatcher.despachar(Eventos.PUJA_ACEPTADA, json.writeValueAsBytes(sobre), 42L, "corr-12345678");
        assertThat(ana.siguiente("PUJA_ACEPTADA").path("secuencia").asLong()).isEqualTo(42);
    }

    @Test
    @DisplayName("OBS-01 · /actuator/prometheus exige la credencial interna; la salud es abierta")
    void metricasProtegidas() throws Exception {
        var cliente = HttpClient.newHttpClient();
        var sin = cliente.send(java.net.http.HttpRequest.newBuilder(URI.create("http://localhost:" + puerto
                + "/actuator/prometheus")).build(), java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(sin.statusCode()).isEqualTo(403);
        var salud = cliente.send(java.net.http.HttpRequest.newBuilder(URI.create("http://localhost:" + puerto
                + "/actuator/health/liveness")).build(), java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(salud.statusCode()).isEqualTo(200);
    }

    @Test
    @DisplayName("OBS-01/DIS-01 · Las métricas llevan la réplica (instancia) y buckets fijos en 800 ms y 1,5 s")
    void metricasPorReplicaConUmbrales() throws Exception {
        var cuerpo = HttpClient.newHttpClient().send(java.net.http.HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + puerto + "/actuator/prometheus"))
                .header("X-Internal-Token", "credencial-de-prueba").build(),
                java.net.http.HttpResponse.BodyHandlers.ofString()).body();

        assertThat(cuerpo).contains("instancia=\"")
                .containsPattern("cafeorbe_realtime_pujas_reenvio_seconds_bucket\\{[^}]*le=\"0\\.8\"")
                .containsPattern("cafeorbe_realtime_pujas_reenvio_seconds_bucket\\{[^}]*le=\"1\\.5\"")
                // Sin el histograma completo (~70 buckets): solo los umbrales y +Inf.
                .doesNotContain("le=\"0.001\"");
    }
}
