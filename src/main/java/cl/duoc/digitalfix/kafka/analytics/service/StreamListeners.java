package cl.duoc.digitalfix.kafka.analytics.service;

import cl.duoc.digitalfix.kafka.analytics.model.MetricaEvento;
import cl.duoc.digitalfix.kafka.analytics.model.OrdenEvento;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class StreamListeners {
    private static final Logger log = LoggerFactory.getLogger(StreamListeners.class);
    private final ObjectMapper mapper;
    private final KafkaTemplate<String,String> kafka;
    private final ResumenService resumen;
    private final String destino;

    public StreamListeners(ObjectMapper mapper, KafkaTemplate<String,String> kafka, ResumenService resumen,
                           @Value("${app.kafka.topics.analytics}") String destino) {
        this.mapper = mapper; this.kafka = kafka; this.resumen = resumen; this.destino = destino;
    }

    @KafkaListener(topics = "${app.kafka.topics.workorders}", groupId = "${app.kafka.groups.workorders}")
    public void escucharOrden(ConsumerRecord<String,String> record) throws Exception {
        OrdenEvento origen;
        try { origen = mapper.readValue(record.value(), OrdenEvento.class); }
        catch (Exception ex) { throw new IllegalArgumentException("JSON de orden inválido", ex); }
        String categoria = ClasificadorOrden.clasificar(origen);
        MetricaEvento metrica = new MetricaEvento(UUID.randomUUID().toString(), origen.eventId(),
                origen.workOrderId(), origen.estado(), categoria, Instant.now().toString());
        // Esperar ACK del broker antes de confirmar el offset de la orden consumida.
        var resultado = kafka.send(destino, String.valueOf(origen.workOrderId()),
                        mapper.writeValueAsString(metrica))
                .get(15, TimeUnit.SECONDS);
        log.info("Orden procesada: orden={}, estado={}, partition={}, offset={}, metricaPartition={}",
                origen.workOrderId(), origen.estado(), record.partition(), record.offset(),
                resultado.getRecordMetadata().partition());
    }

    @KafkaListener(topics = "${app.kafka.topics.analytics}", groupId = "${app.kafka.groups.analytics}")
    public void escucharMetrica(ConsumerRecord<String,String> record) throws Exception {
        MetricaEvento metrica;
        try { metrica = mapper.readValue(record.value(), MetricaEvento.class); }
        catch (Exception ex) { throw new IllegalArgumentException("JSON de métrica inválido", ex); }
        resumen.registrar(metrica);
        log.info("METRICA procesada: orden={}, categoria={}, partition={}, offset={}",
                metrica.workOrderId(), metrica.categoria(), record.partition(), record.offset());
    }
}