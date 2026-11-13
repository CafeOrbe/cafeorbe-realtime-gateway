package com.cafeorbe.realtime.seguridad;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.boot.logging.logback.StructuredLogEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * OBS-01: escribe lo que llega a los loggers indicados con el mismo codificador que en Azure (JSON logstash de
 * Spring Boot) y guarda las líneas y los errores. No depende del formato con que se configuró el logging de la
 * JVM de pruebas: logback se configura una sola vez y el perfil test usa texto plano, que no detecta un campo
 * repetido. Con JSON, un campo repetido hace fallar al codificador y el evento se pierde sin aviso.
 */
public final class LogsJson extends AppenderBase<ILoggingEvent> implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final StructuredLogEncoder codificador = new StructuredLogEncoder();
    private final List<Logger> loggers;
    public final List<String> lineas = new CopyOnWriteArrayList<>();
    public final List<Exception> errores = new CopyOnWriteArrayList<>();

    private LogsJson(List<Logger> loggers) {
        this.loggers = loggers;
    }

    public static LogsJson de(String... nombres) {
        LoggerContext contexto = (LoggerContext) LoggerFactory.getILoggerFactory();
        var captura = new LogsJson(java.util.Arrays.stream(nombres).map(contexto::getLogger).toList());
        captura.codificador.setContext(contexto);
        captura.codificador.setFormat("logstash");
        captura.codificador.start();
        captura.setContext(contexto);
        captura.start();
        captura.loggers.forEach(l -> l.addAppender(captura));
        return captura;
    }

    @Override
    protected void append(ILoggingEvent evento) {
        try {
            lineas.add(new String(codificador.encode(evento), StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            errores.add(e);
        }
    }

    /** La línea JSON del evento: debe existir, ser JSON válido y llevar el correlationId una sola vez. */
    public void registradoUnaVez(String evento, String correlacion) throws Exception {
        assertThat(errores).as("errores al escribir el log JSON").isEmpty();
        String linea = lineas.stream()
                .filter(l -> l.contains("\"evento\":\"" + evento + "\""))
                .findFirst()
                .orElseThrow(() -> new AssertionError("El evento " + evento + " no quedó en el log JSON"));
        assertThat(JSON.readTree(linea).path("correlationId").asText()).isEqualTo(correlacion);
        assertThat(linea.split("\"correlationId\"", -1)).hasSize(2);
    }

    @Override
    public void close() {
        loggers.forEach(l -> l.detachAppender(this));
        stop();
    }
}
