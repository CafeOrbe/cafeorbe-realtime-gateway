package com.cafeorbe.realtime.presence;

import com.cafeorbe.realtime.backplane.Difusor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Avisa a cada sala cuántos conectados tiene (HU-05, HU-15), agrupando los cambios.
 *
 * <p>Antes cada entrada o salida contaba la sala y enviaba el número a todos al instante. Cuando entran N personas
 * casi a la vez (empieza la subasta, o todos reconectan tras un despliegue) eso son N conteos y N² mensajes. Aquí
 * solo se anota que la sala cambió; una pasada periódica la cuenta una vez y envía un único aviso con el número final.
 */
@Component
public class AvisoDeConectados {

    private static final Logger log = LoggerFactory.getLogger(AvisoDeConectados.class);

    private final Presencia presencia;
    private final Difusor difusor;
    private final Set<UUID> salasConCambios = ConcurrentHashMap.newKeySet();

    public AvisoDeConectados(Presencia presencia, Difusor difusor) {
        this.presencia = presencia;
        this.difusor = difusor;
    }

    /** Alguien entró o salió de la sala: su conteo se enviará en la próxima pasada. */
    public void salaCambio(UUID subastaId) {
        salasConCambios.add(subastaId);
    }

    /** @return cantidad de salas avisadas en esta pasada */
    @Scheduled(fixedDelayString = "${cafeorbe.realtime.conectados-intervalo-ms:1000}")
    public int avisar() {
        int avisadas = 0;
        for (UUID subastaId : Set.copyOf(salasConCambios)) {
            // Se retira antes de contar: un cambio que llegue mientras tanto vuelve a anotar la sala.
            salasConCambios.remove(subastaId);
            try {
                difusor.aSala(subastaId, "CONECTADOS", Map.of("conectados", presencia.contar(subastaId)));
                avisadas++;
            } catch (RuntimeException e) {
                // Redis no respondió: se reintenta en la siguiente pasada en lugar de perder el aviso.
                salasConCambios.add(subastaId);
                log.warn("No se pudo avisar el conteo de la sala {}: {}", subastaId, e.getMessage());
            }
        }
        return avisadas;
    }
}
