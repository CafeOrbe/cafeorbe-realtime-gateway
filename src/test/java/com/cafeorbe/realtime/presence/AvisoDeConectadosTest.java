package com.cafeorbe.realtime.presence;

import com.cafeorbe.realtime.backplane.Backplane;
import com.cafeorbe.realtime.backplane.Difusor;
import com.cafeorbe.realtime.ws.Sobre;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El conteo de conectados se agrupa: muchas entradas seguidas producen un solo aviso con el número final,
 * no un aviso (y un recuento de toda la sala) por cada persona.
 */
class AvisoDeConectadosTest {

    static final UUID SALA = UUID.randomUUID();
    static final UUID OTRA_SALA = UUID.randomUUID();

    private final List<Sobre> enviados = new ArrayList<>();
    private final Backplane backplane = enviados::add;
    private final Difusor difusor = new Difusor(backplane, new ObjectMapper());

    /** Presencia de memoria que cuenta cuántas veces se le pidió recorrer una sala. */
    private static class PresenciaContada extends PresenciaEnMemoria {
        int recuentos;
        boolean caida;

        @Override
        public synchronized int contar(UUID subastaId) {
            if (caida) {
                throw new IllegalStateException("Redis no responde");
            }
            recuentos++;
            return super.contar(subastaId);
        }
    }

    private final PresenciaContada presencia = new PresenciaContada();
    private final AvisoDeConectados aviso = new AvisoDeConectados(presencia, difusor);

    private void entra(UUID sala, String sesion) {
        presencia.anotar(sala, sesion, UUID.randomUUID());
        aviso.salaCambio(sala);
    }

    private int conectadosEn(Sobre sobre) {
        return sobre.datos().get("conectados").asInt();
    }

    @Test
    @DisplayName("Cien entradas seguidas producen un solo aviso con el total y un solo recuento de la sala")
    void entradasAgrupadas() {
        for (int i = 0; i < 100; i++) {
            entra(SALA, "s" + i);
        }
        assertThat(enviados).isEmpty();

        assertThat(aviso.avisar()).isEqualTo(1);

        assertThat(enviados).hasSize(1);
        assertThat(enviados.getFirst().tipo()).isEqualTo("CONECTADOS");
        assertThat(enviados.getFirst().subastaId()).isEqualTo(SALA);
        assertThat(enviados.getFirst().usuarioId()).isNull();
        assertThat(conectadosEn(enviados.getFirst())).isEqualTo(100);
        assertThat(presencia.recuentos).isEqualTo(1);
    }

    @Test
    @DisplayName("Sin cambios no se envía nada, y cada sala recibe solo su propio conteo")
    void soloLasSalasQueCambiaron() {
        assertThat(aviso.avisar()).isZero();

        entra(SALA, "s1");
        entra(SALA, "s2");
        entra(OTRA_SALA, "s3");
        assertThat(aviso.avisar()).isEqualTo(2);

        assertThat(enviados).hasSize(2);
        assertThat(enviados).anySatisfy(s -> {
            assertThat(s.subastaId()).isEqualTo(SALA);
            assertThat(conectadosEn(s)).isEqualTo(2);
        });
        assertThat(enviados).anySatisfy(s -> {
            assertThat(s.subastaId()).isEqualTo(OTRA_SALA);
            assertThat(conectadosEn(s)).isEqualTo(1);
        });

        // La sala ya avisada no se repite hasta que vuelva a cambiar.
        assertThat(aviso.avisar()).isZero();
        presencia.retirar(SALA, "s1");
        aviso.salaCambio(SALA);
        aviso.avisar();
        assertThat(conectadosEn(enviados.getLast())).isEqualTo(1);
    }

    @Test
    @DisplayName("Si no se puede contar la sala, el aviso no se pierde: se envía en la siguiente pasada")
    void reintentaSiFallaElConteo() {
        entra(SALA, "s1");
        presencia.caida = true;

        assertThat(aviso.avisar()).isZero();
        assertThat(enviados).isEmpty();

        presencia.caida = false;
        assertThat(aviso.avisar()).isEqualTo(1);
        assertThat(conectadosEn(enviados.getFirst())).isEqualTo(1);
    }
}
