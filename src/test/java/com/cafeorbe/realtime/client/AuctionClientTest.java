package com.cafeorbe.realtime.client;

import com.cafeorbe.contracts.Cabeceras;
import com.cafeorbe.contracts.Rol;
import com.cafeorbe.realtime.ws.Identidad;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * El realtime no decide nada sobre una puja: la reenvía a auction y translates lo que auction responda.
 * Se prueba contra un auction de mentira por HTTP real (mismo patrón que GatewayTest) y no contra un
 * RestClient simulado, porque lo que se verifica aquí es justo su traducción de errores.
 */
class AuctionClientTest {

    static final UUID SUBASTA = UUID.randomUUID();
    static final Identidad ANA = new Identidad(UUID.randomUUID(), "José Ñandú", Rol.SUBASTADOR);

    private static HttpServer auction;
    private static int puertoCaido;

    /** Código con el que el auction de mentira responde a la próxima petición. */
    private static final AtomicInteger codigo = new AtomicInteger(200);
    private static final AtomicReference<String> cuerpo = new AtomicReference<>("");
    private static final AtomicReference<String> idRecibido = new AtomicReference<>();
    private static final AtomicReference<String> nombreRecibido = new AtomicReference<>();
    private static final AtomicReference<String> rolRecibido = new AtomicReference<>();

    @BeforeAll
    static void levantarAuctionFalso() throws IOException {
        auction = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        auction.createContext("/", intercambio -> {
            idRecibido.set(intercambio.getRequestHeaders().getFirst(Cabeceras.USUARIO_ID));
            nombreRecibido.set(URLDecoder.decode(
                    intercambio.getRequestHeaders().getFirst(Cabeceras.USUARIO_NOMBRE), StandardCharsets.UTF_8));
            rolRecibido.set(intercambio.getRequestHeaders().getFirst(Cabeceras.USUARIO_ROL));
            byte[] bytes = cuerpo.get().getBytes(StandardCharsets.UTF_8);
            intercambio.sendResponseHeaders(codigo.get(), bytes.length);
            try (OutputStream salida = intercambio.getResponseBody()) {
                salida.write(bytes);
            }
        });
        auction.start();
        try (ServerSocket s = new ServerSocket(0)) {
            puertoCaido = s.getLocalPort();
        }
    }

    @AfterAll
    static void apagar() {
        auction.stop(0);
    }

    private static AuctionClient cliente(String url) {
        return new AuctionClient(url, 500, 500, new ObjectMapper());
    }

    private static AuctionClient vivo() {
        return cliente("http://localhost:" + auction.getAddress().getPort());
    }

    @BeforeEach
    void limpiar() {
        codigo.set(200);
        cuerpo.set("");
    }

    @Test
    @DisplayName("pujar(): una puja aceptada se reenvía con la identidad del usuario y su monto")
    void pujaAceptada() {
        vivo().pujar(SUBASTA, ANA, 500);

        assertThat(idRecibido.get()).isEqualTo(ANA.usuarioId().toString());
        assertThat(nombreRecibido.get()).isEqualTo("José Ñandú");
        assertThat(rolRecibido.get()).isEqualTo("SUBASTADOR");
    }

    @Test
    @DisplayName("pujar(): 422 no es un fallo: la puja se procesa y el motivo llega luego por evento")
    void pujaRechazadaConMotivo() {
        codigo.set(422);
        cuerpo.set("{\"mensaje\":\"oferta superada\"}");

        assertThatCode(() -> vivo().pujar(SUBASTA, ANA, 100)).doesNotThrowAnyException();

        assertThat(idRecibido.get()).isEqualTo(ANA.usuarioId().toString());
    }

    @Test
    @DisplayName("pujar(): un 4xx con mensaje de auction se muestra tal cual a quien pujó")
    void pujaConErrorDeNegocio() {
        codigo.set(409);
        cuerpo.set("{\"mensaje\":\"ya no puedes pujar: la subasta terminó\"}");

        assertThatThrownBy(() -> vivo().pujar(SUBASTA, ANA, 100))
                .isInstanceOf(AuctionClient.PujaNoEnviadaException.class)
                .hasMessage("ya no puedes pujar: la subasta terminó");
    }

    @Test
    @DisplayName("pujar(): un cuerpo que no es JSON cae al mensaje genérico, no propaga el error de parseo")
    void errorConCuerpoNoJson() {
        codigo.set(500);
        cuerpo.set("<html>502 Bad Gateway</html>");

        assertThatThrownBy(() -> vivo().pujar(SUBASTA, ANA, 100))
                .isInstanceOf(AuctionClient.PujaNoEnviadaException.class)
                .hasMessage("No se pudo enviar la puja, intenta de nuevo");
    }

    @Test
    @DisplayName("pujar(): un JSON sin campo mensaje también cae al mensaje genérico")
    void errorSinCampoMensaje() {
        codigo.set(400);
        cuerpo.set("{\"otra_cosa\":\"irrelevante\"}");

        assertThatThrownBy(() -> vivo().pujar(SUBASTA, ANA, 100))
                .isInstanceOf(AuctionClient.PujaNoEnviadaException.class)
                .hasMessage("No se pudo enviar la puja, intenta de nuevo");
    }

    @Test
    @DisplayName("pujar(): auction caído da un mensaje genérico, no una excepción técnica")
    void auctionCaido() {
        AuctionClient caido = cliente("http://localhost:" + puertoCaido);

        assertThatThrownBy(() -> caido.pujar(SUBASTA, ANA, 100))
                .isInstanceOf(AuctionClient.PujaNoEnviadaException.class)
                .hasMessage("No se pudo enviar la puja, intenta de nuevo");
    }

    @Test
    @DisplayName("noExiste(): auction confirma 404, la subasta no existe")
    void subastaNoExiste() {
        codigo.set(404);

        assertThat(vivo().noExiste(SUBASTA, ANA)).isTrue();
        assertThat(idRecibido.get()).isEqualTo(ANA.usuarioId().toString());
    }

    @Test
    @DisplayName("noExiste(): la subasta existe (200) o hay otro error, se deja entrar igualmente")
    void subastaExisteOErrorDeRed() {
        codigo.set(200);
        assertThat(vivo().noExiste(SUBASTA, ANA)).isFalse();

        codigo.set(503);
        assertThat(vivo().noExiste(SUBASTA, ANA)).isFalse();

        AuctionClient caido = cliente("http://localhost:" + puertoCaido);
        assertThat(caido.noExiste(SUBASTA, ANA)).isFalse();
    }
}