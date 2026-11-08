package cl.duoc.digitalfix.kafka.analytics.controller;

import cl.duoc.digitalfix.kafka.analytics.service.ResumenService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final ResumenService resumen;

    public AnalyticsController(ResumenService resumen) {
        this.resumen = resumen;
    }

    @GetMapping("/resumen")
    public Map<String, Object> resumen() {
        return resumen.resumen();
    }
}