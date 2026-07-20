package com.epcc.audiencias;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Mantiene estado por usuario (keyBy user_id) y, al detectar el cierre de
 * sesion (evento LOGOUT), evalua las reglas de audiencias digitales
 * definidas en docs/arquitectura.md y emite el resultado.
 *
 * Reglas:
 * - ALTA_INTENCION_COMPRA / ABANDONO_CARRITO: agrego productos al carrito
 *   pero no completo ninguna compra en la sesion.
 * - CLIENTE_FRECUENTE: 3 o mas compras completadas (historico acumulado).
 * - CLIENTE_PREMIUM: alguna compra de la sesion supero el umbral de valor.
 * - INTERESADO_TECNOLOGIA: al menos 60% de sus vistas fueron de Electronics.
 * - RIESGO_ABANDONO: navego bastante (>=5 vistas) pero nunca agrego al carrito.
 */
public class ClasificadorAudiencias extends KeyedProcessFunction<String, Evento, AudienciaResultado> {

    private static final double UMBRAL_CLIENTE_PREMIUM = 1500.0;
    private static final int MIN_COMPRAS_CLIENTE_FRECUENTE = 3;
    private static final double UMBRAL_INTERES_TECNOLOGIA = 0.6;
    private static final int MIN_VISTAS_RIESGO_ABANDONO = 5;

    private transient ValueState<Integer> vistas;
    private transient ValueState<Integer> vistasElectronics;
    private transient ValueState<Integer> agregadosCarrito;
    private transient ValueState<Integer> carritoActivo;
    private transient ValueState<Double> maxCompraSesion;
    private transient ValueState<Integer> comprasHistoricas;
    private transient ValueState<String> tipoAgente;

    @Override
    public void open(Configuration parametros) {
        vistas = getRuntimeContext().getState(new ValueStateDescriptor<>("vistas", Types.INT));
        vistasElectronics = getRuntimeContext().getState(new ValueStateDescriptor<>("vistasElectronics", Types.INT));
        agregadosCarrito = getRuntimeContext().getState(new ValueStateDescriptor<>("agregadosCarrito", Types.INT));
        carritoActivo = getRuntimeContext().getState(new ValueStateDescriptor<>("carritoActivo", Types.INT));
        maxCompraSesion = getRuntimeContext().getState(new ValueStateDescriptor<>("maxCompraSesion", Types.DOUBLE));
        comprasHistoricas = getRuntimeContext().getState(new ValueStateDescriptor<>("comprasHistoricas", Types.INT));
        tipoAgente = getRuntimeContext().getState(new ValueStateDescriptor<>("tipoAgente", Types.STRING));
    }

    @Override
    public void processElement(Evento evento, Context ctx, Collector<AudienciaResultado> out) throws Exception {
        tipoAgente.update(evento.agent_type);

        int v = valorOCero(vistas.value());
        int vE = valorOCero(vistasElectronics.value());
        int ac = valorOCero(agregadosCarrito.value());
        int ca = valorOCero(carritoActivo.value());
        double maxCompra = maxCompraSesion.value() == null ? 0.0 : maxCompraSesion.value();
        int comprasTotales = valorOCero(comprasHistoricas.value());

        switch (evento.event) {
            case "VIEW_PRODUCT":
            case "SEARCH":
                v++;
                if ("Electronics".equals(evento.category)) {
                    vE++;
                }
                break;
            case "ADD_CART":
                ac++;
                ca++;
                break;
            case "REMOVE_CART":
                ca = Math.max(0, ca - 1);
                break;
            case "PURCHASE":
                comprasTotales++;
                if (evento.price != null && evento.price > maxCompra) {
                    maxCompra = evento.price;
                }
                ca = 0; // se vacio el carrito al comprar
                break;
            case "LOGOUT":
                List<String> audienciasDetectadas = evaluarReglas(v, vE, ac, ca, maxCompra, comprasTotales);
                if (!audienciasDetectadas.isEmpty()) {
                    out.collect(new AudienciaResultado(evento.user_id, evento.agent_type,
                            audienciasDetectadas, evento.timestamp));
                }
                // se reinicia el estado de la sesion, pero se conserva el historico de compras
                v = 0; vE = 0; ac = 0; ca = 0; maxCompra = 0.0;
                break;
            default:
                break;
        }

        vistas.update(v);
        vistasElectronics.update(vE);
        agregadosCarrito.update(ac);
        carritoActivo.update(ca);
        maxCompraSesion.update(maxCompra);
        comprasHistoricas.update(comprasTotales);
    }

    private List<String> evaluarReglas(int vistasTotales, int vistasElectronicsTotales, int agregadosAlCarrito,
                                        int carritoActivoFinal, double maxCompraSesion, int comprasHistoricasTotales) {
        List<String> resultado = new ArrayList<>();

        if (agregadosAlCarrito > 0 && carritoActivoFinal > 0) {
            resultado.add("ALTA_INTENCION_COMPRA / ABANDONO_CARRITO");
        }

        if (comprasHistoricasTotales >= MIN_COMPRAS_CLIENTE_FRECUENTE) {
            resultado.add("CLIENTE_FRECUENTE");
        }

        if (maxCompraSesion > UMBRAL_CLIENTE_PREMIUM) {
            resultado.add("CLIENTE_PREMIUM");
        }

        if (vistasTotales > 0 && (double) vistasElectronicsTotales / vistasTotales >= UMBRAL_INTERES_TECNOLOGIA) {
            resultado.add("INTERESADO_TECNOLOGIA");
        }

        if (vistasTotales >= MIN_VISTAS_RIESGO_ABANDONO && agregadosAlCarrito == 0) {
            resultado.add("RIESGO_ABANDONO");
        }

        return resultado;
    }

    private int valorOCero(Integer valor) {
        return valor == null ? 0 : valor;
    }
}
