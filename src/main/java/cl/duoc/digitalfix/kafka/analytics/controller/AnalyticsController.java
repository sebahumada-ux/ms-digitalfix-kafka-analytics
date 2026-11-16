package cl.duoc.digitalfix.kafka.analytics.controller;

import cl.duoc.digitalfix.kafka.analytics.service.ResumenService;
import cl.duoc.digitalfix.kafka.analytics.service.EventosEnVivoService;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final ResumenService resumen;
    private final EventosEnVivoService eventosEnVivo;

    public AnalyticsController(ResumenService resumen, EventosEnVivoService eventosEnVivo) {
        this.resumen = resumen;
        this.eventosEnVivo = eventosEnVivo;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return eventosEnVivo.conectar();
    }

    @GetMapping("/resumen")
    public Map<String, Object> resumen() {
        return resumen.resumen();
    }
}