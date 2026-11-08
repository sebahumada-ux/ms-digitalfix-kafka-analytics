package cl.duoc.digitalfix.kafka.analytics.service;

import cl.duoc.digitalfix.kafka.analytics.model.MetricaEvento;
import org.springframework.stereotype.Service;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

@Service
public class ResumenService {

    private final Set<String> procesados =
            ConcurrentHashMap.newKeySet();

    private final Map<String, LongAdder> contadores =
            new ConcurrentHashMap<>();

    private final Deque<MetricaEvento> recientes =
            new ConcurrentLinkedDeque<>();

    public void registrar(MetricaEvento evento) {

        if (evento == null ||
                evento.eventId() == null ||
                evento.eventId().isBlank() ||
                evento.categoria() == null ||
                evento.workOrderId() == null ||
                evento.workOrderId() <= 0) {

            throw new IllegalArgumentException("Métrica inválida");
        }

        // Evita procesar dos veces el mismo evento.
        if (!procesados.add(evento.eventId())) {
            return;
        }

        // Incrementa el contador de la categoría.
        contadores.computeIfAbsent(
                evento.categoria(),
                c -> new LongAdder()
        ).increment();

        // Guarda los últimos 20 eventos.
        recientes.addFirst(evento);

        while (recientes.size() > 20) {
            recientes.pollLast();
        }
    }

    public Map<String, Object> resumen() {

        Map<String, Long> copia = new TreeMap<>();

        contadores.forEach(
                (clave, valor) -> copia.put(clave, valor.sum())
        );

        return Map.of(
                "totalEventos", procesados.size(),
                "porCategoria", copia,
                "recientes", List.copyOf(recientes)
        );
    }
}