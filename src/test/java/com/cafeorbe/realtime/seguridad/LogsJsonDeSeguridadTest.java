package com.cafeorbe.realtime.seguridad;

import com.cafeorbe.realtime.client.AuctionClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.UUID;

/**
 * SEG-03 / OBS-01: las infracciones se registran mientras se procesa un mensaje, con el correlationId ya en el MDC.
 * Si la auditoría lo agregara otra vez, el campo saldría repetido en el log JSON de Azure y el evento se perdería
 * sin aviso (ver LogsJson).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class LogsJsonDeSeguridadTest {

    @Autowired Auditoria auditoria;
    @MockitoBean AuctionClient auction;

    @Test
    @DisplayName("SEG-03 · Una infracción registrada con el correlationId en el MDC queda en el log JSON una sola vez")
    void infraccionConMdc() throws Exception {
        try (var logs = LogsJson.de("cafeorbe.seguridad")) {
            MDC.put("correlationId", "corr-mensaje-1");
            try {
                auditoria.evento("infraccion", "limite_pujas_por_segundo", UUID.randomUUID(), "203.0.113.7", "corr-mensaje-1");
            } finally {
                MDC.remove("correlationId");
            }
            logs.registradoUnaVez("infraccion", "corr-mensaje-1");
        }
    }

    @Test
    @DisplayName("SEG-04 · Sin MDC (handshake) el correlationId del evento igual queda en el log JSON")
    void rechazoSinMdc() throws Exception {
        try (var logs = LogsJson.de("cafeorbe.seguridad")) {
            auditoria.evento("conexion_rechazada", "ticket_invalido_usado_o_vencido", null, "203.0.113.7", "corr-handshake-1");
            logs.registradoUnaVez("conexion_rechazada", "corr-handshake-1");
        }
    }
}
