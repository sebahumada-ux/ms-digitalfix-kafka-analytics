package cl.duoc.digitalfix.kafka.analytics.model;

public record OrdenEvento(String eventId, Long workOrderId, Long clienteId,
                          String estado, Long tecnicoId, String timestamp) {}