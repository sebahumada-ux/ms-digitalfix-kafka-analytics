package cl.duoc.digitalfix.kafka.analytics;

import cl.duoc.digitalfix.kafka.analytics.model.OrdenEvento;
import cl.duoc.digitalfix.kafka.analytics.service.ClasificadorOrden;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClasificadorOrdenTest {
    @Test void clasificaCierre() {
        assertEquals("ORDEN_CERRADA", ClasificadorOrden.clasificar(
                new OrdenEvento("ev-1",165L,5L,"CERRADA",888L,"2026-11-07T21:22:08Z")));
    }
    @Test void validaEstado() {
        assertThrows(IllegalArgumentException.class, () -> ClasificadorOrden.clasificar(
                new OrdenEvento("ev-1",165L,5L,"INEXISTENTE",888L,"2026-11-07T21:22:08Z")));
    }
    @Test void validaFecha() {
        assertThrows(IllegalArgumentException.class, () -> ClasificadorOrden.clasificar(
                new OrdenEvento("ev-1",165L,5L,"CERRADA",888L,"fecha-rara")));
    }
}