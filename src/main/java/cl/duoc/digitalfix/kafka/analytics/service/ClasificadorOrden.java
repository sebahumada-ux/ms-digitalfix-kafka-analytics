package cl.duoc.digitalfix.kafka.analytics.service;

import cl.duoc.digitalfix.kafka.analytics.model.OrdenEvento;

import java.time.Instant;
import java.util.Set;

public final class ClasificadorOrden {

    private static final Set<String> ESTADOS = Set.of(
            "CREADA",
            "ASIGNADA",
            "EN_DESPLAZAMIENTO",
            "EN_EJECUCION",
            "CERRADA",
            "CANCELADA"
    );

    private ClasificadorOrden() {
    }

    public static String clasificar(OrdenEvento evento) {

        if (evento.eventId() == null ||
                evento.eventId().isBlank() ||
                evento.workOrderId() == null ||
                evento.workOrderId() <= 0 ||
                evento.clienteId() == null ||
                evento.clienteId() <= 0 ||
                evento.estado() == null ||
                !ESTADOS.contains(evento.estado())) {

            throw new IllegalArgumentException(
                    "Evento de orden inválido"
            );
        }

        try {
            Instant.parse(evento.timestamp());
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                    "Timestamp inválido", ex
            );
        }

        return switch (evento.estado()) {
            case "CERRADA" -> "ORDEN_CERRADA";
            case "ASIGNADA" -> "ORDEN_ASIGNADA";
            case "CANCELADA" -> "ORDEN_CANCELADA";
            default -> "CAMBIO_ESTADO";
        };
    }
}