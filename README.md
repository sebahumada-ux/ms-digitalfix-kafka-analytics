# DigitalFix - Microservicio Kafka Analytics

Microservicio encargado del procesamiento, clasificación y análisis de eventos de órdenes de trabajo del proyecto **DigitalFix** mediante Apache Kafka.

El microservicio consume eventos generados por Workorders, valida la información recibida, clasifica los estados de las órdenes y publica métricas en un tópico de analítica.

Las métricas pueden consultarse mediante una API REST y visualizarse en el dashboard de DigitalFix, el cual incorpora actualización automática en tiempo real mediante Server-Sent Events (SSE).

## Integrantes

- Sebastián Ahumada
- Benjamín Gutiérrez

## Tecnologías utilizadas

- Java 21
- Spring Boot
- Spring Kafka
- Apache Kafka
- Apache ZooKeeper
- Maven
- Docker
- Docker Compose
- Kafka UI
- Server-Sent Events (SSE)
- Angular
- Nginx
- Caddy
- AWS EC2
- HTTPS

## Funcionalidades implementadas

El microservicio permite:

- Consumir eventos de órdenes de trabajo.
- Validar la información recibida.
- Clasificar eventos según el estado de la orden.
- Generar y publicar métricas.
- Consumir eventos de analítica.
- Consultar estadísticas mediante una API REST.
- Registrar los últimos 20 eventos procesados.
- Evitar el procesamiento duplicado de métricas en memoria.
- Gestionar mensajes inválidos mediante Dead Letter Topics.
- Utilizar mecanismos de reintentos y recuperación.
- Enviar notificaciones mediante Server-Sent Events.
- Actualizar el dashboard de DigitalFix en tiempo real.
- Integrarse con Workorders y Kafka Admin.
- Ejecutarse mediante Docker Compose en AWS.

## Arquitectura

Kafka Analytics forma parte de la arquitectura de procesamiento asíncrono de eventos de DigitalFix.

```text
              DigitalFix Frontend
                       |
                       v
                 API Gateway
                       |
                       v
                      BFF
                       |
                       v
                  Workorders
                       |
                       | Kafka Producer
                       v
           digitalfix.ordenes.eventos
                       |
                       | Kafka Consumer
                       v
               Kafka Analytics
                    :8091
                       |
                       v
            Validación y clasificación
                       |
                       | Kafka Producer
                       v
          digitalfix.analitica.eventos
                       |
                       | Kafka Consumer
                       v
              Registro de métricas
                       |
            +----------+----------+
            |                     |
            v                     v
         API REST                SSE
            |                     |
            +----------+----------+
                       |
                       v
                     Nginx
                       |
                       v
              Dashboard Angular
```

Workorders genera eventos cuando se crean órdenes de trabajo o se modifican sus estados.

Kafka Analytics procesa los eventos y genera métricas que posteriormente se utilizan para actualizar el dashboard.

Kafka Admin también permite publicar eventos de demostración para verificar el funcionamiento del microservicio.

## Flujo de procesamiento

El procesamiento de eventos considera las siguientes etapas:

1. El usuario crea o modifica una orden de trabajo.
2. Workorders registra la operación en Oracle Database.
3. Workorders publica un evento en Apache Kafka.
4. Kafka Analytics consume el evento.
5. Se valida la información recibida.
6. El evento se clasifica según el estado de la orden.
7. Se genera una métrica.
8. La métrica se publica en el tópico de analítica.
9. El consumidor de métricas registra la información.
10. Se actualizan las estadísticas del microservicio.
11. Se envía una notificación SSE.
12. El dashboard consulta y presenta la información actualizada.

La persistencia de las órdenes de trabajo corresponde a Workorders y Oracle Database. Kafka Analytics se encarga del procesamiento de eventos y las métricas.

## Infraestructura Kafka

La infraestructura está compuesta por:

- 3 brokers Apache Kafka.
- 3 nodos Apache ZooKeeper.
- 1 interfaz Kafka UI.
- 1 microservicio Kafka Admin.
- 1 microservicio Kafka Analytics.

Los brokers permiten distribuir eventos mediante particiones y mantener copias mediante replicación.

Kafka Analytics utiliza la infraestructura compartida para consumir y publicar eventos.

## Estados de una orden

Los estados utilizados son:

- CREADA
- ASIGNADA
- EN_DESPLAZAMIENTO
- EN_EJECUCION
- CERRADA
- CANCELADA

## Clasificación de eventos

Los eventos recibidos se clasifican mediante las siguientes categorías:

| Estado | Categoría |
|---|---|
| CERRADA | ORDEN_CERRADA |
| ASIGNADA | ORDEN_ASIGNADA |
| CANCELADA | ORDEN_CANCELADA |
| Otros estados válidos | CAMBIO_ESTADO |

La clasificación permite generar estadísticas relacionadas con las operaciones de las órdenes de trabajo.

Las métricas representan eventos procesados y no necesariamente la cantidad de órdenes únicas existentes en Oracle Database.

Una misma orden puede generar múltiples eventos durante su ciclo de vida.

## Tópicos Kafka

### Tópico de órdenes de trabajo

`digitalfix.ordenes.eventos`

Recibe los eventos generados por Workorders o Kafka Admin.

El consumidor de Kafka Analytics procesa y clasifica los mensajes recibidos.

### Tópico de analítica

`digitalfix.analitica.eventos`

Almacena las métricas generadas a partir de los eventos de órdenes.

Kafka Analytics publica y consume mensajes de este tópico.

### Dead Letter Topics

- `digitalfix.ordenes.eventos.DLT`
- `digitalfix.analitica.eventos.DLT`

Los tópicos DLT reciben mensajes que no pudieron procesarse correctamente mediante los mecanismos de recuperación configurados.

## Configuración de tópicos

La configuración inicial contempla:

| Parámetro | Valor |
|---|---|
| Particiones | 3 |
| Factor de replicación | 3 |
| Réplicas mínimas sincronizadas | 2 |
| Retención | 7 días |
| Política de limpieza | delete |

La creación y configuración de los tópicos se encuentra en:

`digitalfix-infra/kafka/scripts/init-topics.sh`

## Productores y consumidores

Kafka Analytics utiliza Spring Kafka para implementar productores y consumidores de mensajes.

### Consumer de órdenes

El consumidor recibe eventos provenientes del tópico:

`digitalfix.ordenes.eventos`

Funciones:

- Recibir eventos mediante `@KafkaListener`.
- Validar la información recibida.
- Identificar el estado de la orden.
- Clasificar el evento.
- Generar métricas.
- Publicar los resultados en Kafka.

### Producer de métricas

El productor publica los resultados en:

`digitalfix.analitica.eventos`

La publicación permite separar el procesamiento de órdenes del registro de métricas.

### Consumer de métricas

El consumidor de métricas recibe los eventos publicados en el tópico de analítica.

Funciones:

- Registrar métricas procesadas.
- Actualizar contadores.
- Registrar los eventos recientes.
- Controlar métricas duplicadas en memoria.
- Generar notificaciones para el dashboard.

## Consumer Groups

El microservicio utiliza dos grupos principales de consumidores.

### Grupo de órdenes

```text
digitalfix-analytics-workorders
```

Responsable de consumir los eventos del tópico:

`digitalfix.ordenes.eventos`

### Grupo de métricas

```text
digitalfix-analytics-metrics
```

Responsable de consumir los eventos del tópico:

`digitalfix.analitica.eventos`

La utilización de grupos permite distribuir el procesamiento entre consumidores y administrar los offsets correspondientes.

## Particiones, keys y offsets

Las particiones permiten distribuir los mensajes dentro de Apache Kafka.

Los eventos de órdenes utilizan el identificador de la orden como key, permitiendo mantener los eventos relacionados en una misma partición mientras no cambie la configuración de particionado.

El offset representa la posición de un mensaje dentro de una partición.

Kafka mantiene los offsets de los grupos de consumidores para controlar el progreso del procesamiento.

Kafka UI y Kafka Admin permiten consultar:

- Particiones.
- Keys.
- Offsets.
- Consumer groups.
- Lag de consumidores.
- Réplicas.
- ISR.

## Puerto

El microservicio utiliza el puerto:

`8091`

Dirección local:

`http://localhost:8091`

En AWS, el puerto se mantiene vinculado a localhost de la instancia EC2. El acceso HTTPS externo se realiza mediante Caddy y el acceso desde el frontend utiliza el proxy configurado en Nginx.

## Endpoints principales

### Consulta de estadísticas

```http
GET /api/analytics/resumen
```

El endpoint permite consultar:

- Total de eventos procesados.
- Cantidad de eventos por categoría.
- Últimos 20 eventos registrados.
- Identificador del evento.
- Identificador del evento de origen.
- Identificador de la orden de trabajo.
- Estado de la orden.
- Categoría del evento.
- Fecha y hora del evento.

### Actualización en tiempo real

```http
GET /api/analytics/stream
```

El endpoint establece una conexión Server-Sent Events para enviar notificaciones hacia el cliente cuando se actualizan las métricas.

La funcionalidad SSE se encuentra integrada en la rama `main` del repositorio.

## Ejemplo de respuesta del endpoint de resumen

El siguiente ejemplo representa la estructura de respuesta obtenida durante la prueba de integración de la orden 221 en AWS.

```json
{
  "recientes": [
    {
      "eventId": "identificador-evento-2",
      "sourceEventId": "identificador-origen-2",
      "workOrderId": 221,
      "estado": "ASIGNADA",
      "categoria": "ORDEN_ASIGNADA",
      "timestamp": "2026-11-23T23:42:06.581935078Z"
    },
    {
      "eventId": "identificador-evento-1",
      "sourceEventId": "identificador-origen-1",
      "workOrderId": 221,
      "estado": "CREADA",
      "categoria": "CAMBIO_ESTADO",
      "timestamp": "2026-11-23T23:41:08.677871099Z"
    }
  ],
  "totalEventos": 2,
  "porCategoria": {
    "CAMBIO_ESTADO": 1,
    "ORDEN_ASIGNADA": 1
  }
}
```

Los identificadores de eventos se muestran de manera ilustrativa.

Los timestamps utilizan el formato ISO 8601. El sufijo `Z` representa la zona horaria UTC.

## Server-Sent Events (SSE)

Server-Sent Events permite mantener una conexión HTTP abierta entre el servidor y el cliente para enviar notificaciones desde el backend hacia el frontend.

En DigitalFix, SSE permite actualizar automáticamente las métricas mostradas en el dashboard cuando se procesan nuevos eventos.

### Flujo de actualización

```text
Kafka Analytics
       |
       v
Nueva métrica procesada
       |
       v
Notificación SSE
       |
       v
Nginx
       |
       v
Dashboard Angular
       |
       v
Consulta del resumen actualizado
       |
       v
Actualización de indicadores
```

Características:

- Comunicación unidireccional desde el servidor hacia el cliente.
- Actualización automática de información.
- Conexión HTTP persistente.
- Integración con el dashboard Angular.
- Reducción de consultas manuales.
- Notificaciones asociadas al procesamiento de eventos Kafka.

El dashboard puede recibir notificaciones sin necesidad de recargar manualmente toda la página.

## Integración con el frontend

El frontend de DigitalFix utiliza Angular y se encuentra desplegado mediante Nginx.

Las solicitudes de Analytics utilizan las siguientes rutas:

```http
GET /api/analytics/resumen
GET /api/analytics/stream
```

Nginx actúa como proxy inverso hacia Kafka Analytics.

### Configuración de referencia

```nginx
location /api/analytics/ {
    proxy_pass http://kafka-analytics:8091;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 3600s;
}
```

La desactivación del buffering permite que las notificaciones SSE se entreguen al cliente sin esperar la acumulación de la respuesta.

El frontend y Kafka Analytics comparten una red Docker para permitir la comunicación interna.

## Dashboard de DigitalFix

El dashboard permite visualizar información relacionada con los eventos de órdenes de trabajo.

Características:

- Total de eventos procesados.
- Distribución de eventos por categoría.
- Eventos recientes.
- Identificación de órdenes de trabajo.
- Estados procesados.
- Actualización automática mediante SSE.

El dashboard consume las métricas generadas por Kafka Analytics y permite demostrar el funcionamiento integrado de Apache Kafka dentro de DigitalFix.

Las funcionalidades de actualización en tiempo real se encuentran integradas en las ramas `main` del frontend y Kafka Analytics.

## Manejo de errores

El microservicio implementa mecanismos de control y recuperación durante el procesamiento de mensajes.

Características:

- Validación de eventos.
- Validación de estructuras JSON.
- Manejo de excepciones.
- Reintentos para errores recuperables.
- Publicación de mensajes fallidos en Dead Letter Topics.
- Registro de errores mediante logs.
- Continuidad del procesamiento ante mensajes inválidos recuperados.

Los errores de validación no reintentables se envían directamente al mecanismo de recuperación configurado.

La configuración del manejo de errores se encuentra en:

`KafkaErrorsConfig.java`

## Dead Letter Topics

Los Dead Letter Topics permiten separar los mensajes que no pudieron procesarse correctamente.

```text
Mensaje recibido
       |
       v
Validación del evento
       |
       +---- Evento válido ------> Procesamiento
       |
       +---- Evento inválido
                   |
                   v
          Manejo de excepciones
                   |
                   v
        Reintentos si corresponde
                   |
                   v
            Dead Letter Topic
```

Los mensajes fallidos pueden consultarse mediante Kafka UI.

La recuperación hacia un DLT permite investigar los errores sin impedir permanentemente el procesamiento de los mensajes posteriores.

## Persistencia

Las métricas se almacenan temporalmente en memoria mediante estructuras concurrentes de Java.

Características:

- Contadores de eventos en memoria.
- Registro de los últimos 20 eventos.
- Control de métricas duplicadas en memoria.
- Consulta rápida de estadísticas.

Los contadores y registros recientes pueden reiniciarse cuando se reinicia el microservicio.

Los offsets de Kafka y los mensajes almacenados en sus tópicos poseen un ciclo de persistencia independiente.

La recuperación automática de todas las métricas históricas no está garantizada al reiniciar Kafka Analytics.

La persistencia de las órdenes de trabajo se realiza mediante Workorders y Oracle Autonomous Database.

## Docker

El proyecto incluye un archivo:

`Dockerfile`

El archivo permite construir una imagen Docker utilizando Java 21.

La configuración Docker Compose se encuentra en el repositorio de infraestructura:

`digitalfix-infra/kafka/docker-compose.yml`

Kafka Analytics se conecta a los siguientes servidores bootstrap dentro de la red Docker:

```text
kafka-1:9092
kafka-2:9092
kafka-3:9092
```

## Compilación

### Windows

```powershell
.\mvnw.cmd clean package
```

### Linux

```bash
./mvnw clean package
```

## Ejecución de pruebas

En Windows:

```powershell
.\mvnw.cmd test
```

En Linux:

```bash
./mvnw test
```

Las pruebas permiten verificar funcionalidades relacionadas con el procesamiento y registro de métricas.

## Ejecución local

Con Kafka disponible y los servidores bootstrap correctamente configurados, ejecutar en Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

En Linux:

```bash
./mvnw spring-boot:run
```

### Consultar estadísticas

```bash
curl http://localhost:8091/api/analytics/resumen
```

### Consultar conexión SSE

```bash
curl -N --max-time 5 http://localhost:8091/api/analytics/stream
```

La conexión SSE permanece abierta mientras el cliente esté conectado.

El parámetro `--max-time 5` permite finalizar la prueba después de cinco segundos. Un timeout generado por este límite no significa necesariamente que el servicio esté fallando.

## Despliegue mediante Docker Compose

Los comandos se ejecutan desde el directorio padre que contiene los repositorios de DigitalFix.

### Iniciar infraestructura Kafka

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml up -d
```

### Inicializar tópicos

Una vez que los brokers estén operativos:

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile tools run --rm kafka-init
```

### Iniciar Kafka Admin y Kafka Analytics

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile apps up -d --build kafka-admin kafka-analytics
```

### Verificar contenedores

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile apps ps
```

### Consultar logs

```bash
docker compose --project-directory . -f digitalfix-infra/kafka/docker-compose.yml --profile apps logs --tail=50 kafka-analytics
```

## Pruebas de funcionamiento

### Publicar un evento válido

Utilizando Kafka Admin:

```bash
curl -X POST http://localhost:8090/api/demo/workorders \
  -H "Content-Type: application/json" \
  -d '{
    "workOrderId": 555,
    "clienteId": 1,
    "estado": "CREADA"
  }'
```

Consultar las métricas:

```bash
curl http://localhost:8091/api/analytics/resumen
```

### Publicar un evento de asignación

```bash
curl -X POST http://localhost:8090/api/demo/workorders \
  -H "Content-Type: application/json" \
  -d '{
    "workOrderId": 555,
    "clienteId": 1,
    "tecnicoId": 2,
    "estado": "ASIGNADA"
  }'
```

El evento debe procesarse y clasificarse como:

`ORDEN_ASIGNADA`

Estos eventos corresponden a datos de demostración y no modifican directamente las órdenes almacenadas en Oracle Database.

### Publicar un mensaje inválido

```bash
curl -X POST http://localhost:8090/api/demo/malformed-event
```

Posteriormente, consultar los logs de Kafka Analytics y verificar los mensajes recuperados mediante Kafka UI en:

`digitalfix.ordenes.eventos.DLT`

### Verificación del dashboard

1. Iniciar el frontend de DigitalFix.
2. Acceder al dashboard de monitoreo.
3. Crear o modificar una orden de trabajo.
4. Verificar la publicación del evento en Kafka.
5. Consultar el procesamiento en Kafka Analytics.
6. Comprobar la actualización de las métricas sin recargar manualmente el dashboard.

## Despliegue en AWS

Kafka Analytics fue desplegado como parte de la infraestructura completa de DigitalFix en una instancia AWS EC2.

Características del entorno:

- Amazon Linux 2023.
- Instancia EC2 t3.large.
- Docker y Docker Compose.
- Clúster Kafka de tres brokers.
- Tres nodos ZooKeeper.
- Kafka UI.
- Kafka Admin.
- Redes Docker compartidas.
- Caddy como proxy inverso HTTPS.
- Nginx como servidor del frontend y proxy de Analytics.

### Arquitectura del despliegue

```text
                   Usuario
                      |
                      | HTTPS
                      v
                    Caddy
                      |
           +----------+----------+
           |                     |
           v                     v
      Angular / Nginx      Kafka Analytics
           |                    :8091
           |                     |
           +---------------------+
                                 |
                                 v
                           Apache Kafka
                                 |
                                 v
                             Workorders
```

El frontend utiliza Nginx para acceder internamente a Kafka Analytics.

Caddy permite consultar los endpoints de Analytics mediante HTTPS y autenticación HTTP básica.

La guía completa del despliegue se encuentra en:

[DigitalFix - Despliegue completo en AWS](https://github.com/sebahumada-ux/digitalfix-infra/blob/main/aws/README.md)

## Seguridad

Consideraciones de seguridad:

- No publicar credenciales ni tokens en el repositorio.
- No exponer directamente los puertos de los brokers Kafka.
- Restringir el acceso a los servicios internos.
- Utilizar HTTPS para el acceso externo.
- Proteger las herramientas publicadas mediante Caddy.
- Mantener las configuraciones privadas fuera del control de versiones.

La protección de Kafka Analytics mediante Caddy es independiente de la autorización JWT utilizada por los microservicios de negocio de DigitalFix.

## Validación del despliegue integrado

El 23 de noviembre de 2026 se realizó una prueba de funcionamiento integrado de DigitalFix en AWS.

Durante la prueba se creó y posteriormente se asignó la orden de trabajo 221.

### Resultados obtenidos

| Componente | Resultado |
|---|---|
| Workorders | Creación y asignación de la orden 221. |
| Oracle Database | Persistencia de las operaciones de negocio. |
| Kafka Producer | Publicación de eventos asociados a la orden. |
| Kafka Consumer | Consumo de los eventos publicados. |
| Kafka Analytics | Clasificación y registro de dos eventos. |
| Kafka UI | Visualización de mensajes, key y offsets. |
| API REST | Consulta del resumen de métricas. |
| SSE | Conexión de eventos en tiempo real. |
| Dashboard Angular | Integración de métricas y actualización automática. |
| Caddy | Acceso mediante HTTPS. |

### Eventos procesados

| Orden | Estado | Categoría |
|---|---|---|
| 221 | CREADA | CAMBIO_ESTADO |
| 221 | ASIGNADA | ORDEN_ASIGNADA |

El resumen de Kafka Analytics registró dos eventos procesados, correspondientes a las operaciones realizadas sobre la misma orden de trabajo.

La prueba permitió comprobar la comunicación entre Workorders, Apache Kafka, Kafka Analytics y el frontend de DigitalFix.

## Consideraciones de disponibilidad

Los brokers Kafka y Kafka Analytics se ejecutan mediante contenedores Docker dentro de una instancia EC2.

La replicación de Kafka permite demostrar tolerancia ante fallos individuales de brokers, pero no garantiza disponibilidad ante la pérdida completa de la instancia EC2.

Los datos de Kafka se almacenan mediante volúmenes persistentes.

Las métricas de Kafka Analytics se almacenan en memoria y pueden reiniciarse al reiniciar el microservicio.

## Repositorios relacionados

### Infraestructura DigitalFix

https://github.com/sebahumada-ux/digitalfix-infra

### Kafka Admin

https://github.com/sebahumada-ux/ms-digitalfix-kafka-admin

### Workorders

https://github.com/sebahumada-ux/digitalfix-ms-workorders

### Frontend DigitalFix

https://github.com/sebahumada-ux/frontend-digitalfix

## Estado del proyecto

Actualmente se encuentran implementados:

- Consumo de eventos Kafka.
- Validación y clasificación de eventos.
- Generación y publicación de métricas.
- Consumer groups independientes.
- Control de métricas duplicadas en memoria.
- Registro de los últimos 20 eventos.
- API REST para consultar estadísticas.
- Manejo de errores y reintentos.
- Recuperación mediante Dead Letter Topics.
- Actualización en tiempo real mediante SSE.
- Integración con el dashboard Angular.
- Configuración mediante Docker Compose.
- Pruebas unitarias con Maven.
- Despliegue en AWS EC2.
- Acceso mediante HTTPS.
- Integración con los microservicios de DigitalFix.
- Código fuente y documentación publicados en GitHub.
