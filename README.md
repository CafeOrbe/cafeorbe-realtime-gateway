# cafeorbe-realtime-gateway

WebSocket por sala. **Arquitectura:** orientada a eventos (publicación / suscripción). Puerto `8085`. No decide nada: recibe hechos del broker y los reparte.

## Protocolo

Conexión: `ws://localhost:8085/ws/salas/{subastaId}?token={JWT}` (un navegador no puede enviar cabeceras en un WebSocket). Token inválido → HTTP 401 en el handshake.

**Cliente → servidor**

| Mensaje | Efecto |
|---|---|
| `{"tipo":"PUJAR","monto":110}` | Solo Comprador. Se reenvía a auction (`POST /api/subastas/{id}/pujas`); el resultado llega como evento. |
| `{"tipo":"PING"}` | Responde `PONG` (el cliente lo envía cada 25 s). |

**Servidor → cliente** (`{"tipo","subastaId","datos"}`)

| `tipo` | A quién | Origen |
|---|---|---|
| `CONECTADOS` | toda la sala | entrar / salir de la sala (usuarios distintos) |
| `SUBASTA_INICIADA` | toda la sala | evento `subasta.iniciada` |
| `PUJA_ACEPTADA` | toda la sala | evento `puja.aceptada` |
| `PUJA_RECHAZADA` | solo quien pujó | evento `puja.rechazada` |
| `TRANSMISION_INICIADA` / `TRANSMISION_DETENIDA` | toda la sala | eventos `transmision.*` |
| `ERROR` | solo quien lo causó | mensaje inválido, sin permiso o auction caído |

## Cómo escala

- **Backplane Redis** (canal `cafeorbe.salas`): un solo consumidor de la cola `realtime.eventos` recibe cada evento y lo publica en Redis; **todas** las instancias lo entregan a sus conexiones locales.
- **Presencia Redis** (`presencia:sala:{id}`, hash sesión → usuario): el conteo cuenta usuarios distintos, no pestañas.
- Estructura: `ws/` conexiones y salas · `handlers/` un manejador por evento · `presence/` Redis · `backplane/` Redis.

`cafeorbe.realtime.modo=memoria` reemplaza Redis por implementaciones locales (pruebas o una sola instancia).

Limitación: si una instancia se cae sin cerrar sus conexiones, sus sesiones quedan en el hash de presencia hasta que expire (6 h sin actividad).

## Ejecutar

```bash
mvn spring-boot:run      # requiere RabbitMQ y Redis
mvn test                 # 11 pruebas con un cliente WebSocket real, sin infraestructura
```
