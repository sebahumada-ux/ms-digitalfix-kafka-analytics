# DigitalFix - Microservicio Kafka Analytics

Microservicio encargado del procesamiento y análisis de eventos de órdenes de trabajo del proyecto DigitalFix mediante Apache Kafka.

## Integrantes

- Sebastián Ahumada
- Benjamín Gutiérrez

## Tecnologías utilizadas

- Java 21
- Spring Boot
- Spring Kafka
- Maven
- Apache Kafka
- Apache ZooKeeper
- Docker
- Docker Compose
- Kafka UI

## Funcionalidades implementadas

El microservicio permite:

- Consumir eventos de órdenes de trabajo.
- Validar la información recibida.
- Clasificar eventos según el estado de la orden.
- Generar y publicar métricas.
- Consultar estadísticas mediante una API REST.
- Registrar los últimos 20 eventos procesados.
- Evitar el procesamiento duplicado de métricas en memoria.
- Gestionar mensajes inválidos mediante Dead Letter Topics (DLT).

## Estados de una orden

Los estados utilizados son:

- CREADA
- ASIGNADA
- EN_DESPLAZAMIENTO
- EN_EJECUCION
- CERRADA
- CANCELADA

## Clasificación de eventos

| Estado | Categoría |
|---|---|
| CERRADA | ORDEN_CERRADA |
| ASIGNADA | ORDEN_ASIGNADA |
| CANCELADA | ORDEN_CANCELADA |
| Otros estados válidos | CAMBIO_ESTADO |

## Tópicos Kafka

El microservicio utiliza:

`digitalfix.ordenes.eventos`

Recibe eventos de las órdenes de trabajo.

`digitalfix.analitica.eventos`

Publica y consume métricas generadas a partir de los eventos.

## Puerto

El microservicio utiliza el puerto:

`8091`

## Endpoint principal

```http
GET /api/analytics/resumen
```

El endpoint permite consultar:

- Total de eventos procesados.
- Cantidad de eventos por categoría.
- Últimos 20 eventos registrados.

## Arquitectura

```text
Kafka Admin :8090
       |
       v
digitalfix.ordenes.eventos
       |
       v
Kafka Analytics :8091
       |
       v
Validación y clasificación
       |
       v
digitalfix.analitica.eventos
       |
       v
Registro de métricas
       |
       v
GET /api/analytics/resumen
```

## Manejo de errores

El microservicio implementa:

- Validación de eventos.
- Manejo de excepciones.
- Reintentos para errores recuperables.
- Publicación de mensajes fallidos en tópicos DLT.
- Registro de errores mediante logs.

Los errores de validación no reintentables se envían directamente al mecanismo de recuperación configurado.

## Persistencia

Las métricas se almacenan temporalmente en memoria mediante estructuras concurrentes de Java.

Los datos se reinician cuando se reinicia el microservicio.

## Docker

El proyecto incluye un archivo:

`Dockerfile`

El microservicio puede ejecutarse como contenedor Docker conectado a la infraestructura Kafka.

## Compilación

En Windows:

```powershell
.\mvnw.cmd clean package
```

Para ejecutar las pruebas:

```powershell
.\mvnw.cmd test
```

## Ejecución local

Con Kafka disponible, en Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

Para consultar las métricas:

```text
http://localhost:8091/api/analytics/resumen
```

## Estado del proyecto

Actualmente se encuentran implementados y comprobados mediante las pruebas disponibles:

- Consumo y clasificación de eventos.
- Generación de métricas Kafka.
- Endpoint de consulta de estadísticas.
- Control de eventos duplicados en memoria.
- Configuración de reintentos y DLT.
- Pruebas unitarias con Maven.
- Compilación con BUILD SUCCESS.
- Código fuente publicado en GitHub.

## Proyecto académico

**Asignatura:** Desarrollo Cloud Native I

**Evaluación:** EV3 - Apache Kafka

**Institución:** Duoc UC
