package cl.duoc.digitalfix.kafka.analytics.model;

public record MetricaEvento(String eventId, String sourceEventId, Long workOrderId,
                            String estado, String categoria, String timestamp) {}