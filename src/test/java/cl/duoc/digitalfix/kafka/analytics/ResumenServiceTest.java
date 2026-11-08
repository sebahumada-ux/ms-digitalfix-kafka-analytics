package cl.duoc.digitalfix.kafka.analytics;

import cl.duoc.digitalfix.kafka.analytics.model.MetricaEvento;
import cl.duoc.digitalfix.kafka.analytics.service.ResumenService;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ResumenServiceTest {
    @Test void ignoraDuplicadosPorId() {
        ResumenService service = new ResumenService();
        var evento = new MetricaEvento("id-1","origen-1",165L,"CERRADA","ORDEN_CERRADA","2026-11-07T21:22:08Z");
        service.registrar(evento);
        service.registrar(evento);
        assertEquals(1, service.resumen().get("totalEventos"));
        assertEquals(1L, ((Map<?,?>)service.resumen().get("porCategoria")).get("ORDEN_CERRADA"));
    }
}