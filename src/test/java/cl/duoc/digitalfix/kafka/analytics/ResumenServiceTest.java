package cl.duoc.digitalfix.kafka.analytics;

import cl.duoc.digitalfix.kafka.analytics.model.MetricaEvento;
import cl.duoc.digitalfix.kafka.analytics.service.ResumenService;
import cl.duoc.digitalfix.kafka.analytics.service.EventosEnVivoService;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ResumenServiceTest {
    @Test void ignoraDuplicadosPorId() {
        EventosEnVivoService eventosEnVivo = mock(EventosEnVivoService.class);
        ResumenService service = new ResumenService(eventosEnVivo);
        var evento = new MetricaEvento("id-1","origen-1",165L,"CERRADA","ORDEN_CERRADA","2026-11-07T21:22:08Z");
        service.registrar(evento);
        service.registrar(evento);
        assertEquals(1, service.resumen().get("totalEventos"));
        assertEquals(1L, ((Map<?,?>)service.resumen().get("porCategoria")).get("ORDEN_CERRADA"));
        verify(eventosEnVivo, times(1)).notificar(evento);
    }
}