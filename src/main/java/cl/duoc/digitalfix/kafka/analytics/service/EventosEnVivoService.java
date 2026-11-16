package cl.duoc.digitalfix.kafka.analytics.service;

import cl.duoc.digitalfix.kafka.analytics.model.MetricaEvento;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class EventosEnVivoService {

    private final Set<SseEmitter> clientes = ConcurrentHashMap.newKeySet();

    public SseEmitter conectar() {
        // EventSource se reconecta automáticamente al caducar esta conexión.
        SseEmitter emisor = new SseEmitter(1_800_000L);
        clientes.add(emisor);

        emisor.onCompletion(() -> clientes.remove(emisor));
        emisor.onTimeout(() -> clientes.remove(emisor));
        emisor.onError(error -> clientes.remove(emisor));

        try {
            emisor.send(SseEmitter.event().name("conectado").data("ok"));
        } catch (IOException ex) {
            clientes.remove(emisor);
            emisor.completeWithError(ex);
        }

        return emisor;
    }

    public void notificar(MetricaEvento evento) {
        for (SseEmitter cliente : clientes) {
            try {
                cliente.send(
                        SseEmitter.event()
                                .name("orden-actualizada")
                                .data(evento)
                );
            } catch (Exception ex) {
                clientes.remove(cliente);
                cliente.complete();
            }
        }
    }
}
