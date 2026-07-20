package com.epcc.audiencias;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Metricas generales de la plataforma, calculadas en ventanas de 30s:
 * - eventos por segundo / eventos por tipo
 * - usuarios activos (distintos)
 * - productos mas vistos y mas comprados
 * - compras por region (ciudad)
 * - tasa de conversion (compras / add_cart)
 */
public class MainMetricas {

    private static final Time VENTANA = Time.seconds(30);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Evento> eventos = FuenteEventos.obtenerStream(env, "flink-audiencias-metricas");

        eventos
                .windowAll(TumblingProcessingTimeWindows.of(VENTANA))
                .process(new ProcessAllWindowFunction<Evento, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Evento> elementos, Collector<String> out) {
                        List<Evento> lista = new ArrayList<>();
                        elementos.forEach(lista::add);

                        int totalEventos = lista.size();
                        double eventosPorSegundo = totalEventos / (VENTANA.toMilliseconds() / 1000.0);

                        Set<String> usuariosActivos = lista.stream()
                                .map(e -> e.user_id).collect(Collectors.toSet());

                        Map<String, Long> eventosPorTipo = lista.stream()
                                .collect(Collectors.groupingBy(e -> e.event, Collectors.counting()));

                        Map<String, Long> vistasPorProducto = lista.stream()
                                .filter(e -> "VIEW_PRODUCT".equals(e.event) && e.product != null)
                                .collect(Collectors.groupingBy(e -> e.product, Collectors.counting()));

                        Map<String, Long> comprasPorProducto = lista.stream()
                                .filter(e -> "PURCHASE".equals(e.event) && e.product != null)
                                .collect(Collectors.groupingBy(e -> e.product, Collectors.counting()));

                        Map<String, Long> comprasPorCiudad = lista.stream()
                                .filter(e -> "PURCHASE".equals(e.event))
                                .collect(Collectors.groupingBy(e -> e.city, Collectors.counting()));

                        long totalAddCart = eventosPorTipo.getOrDefault("ADD_CART", 0L);
                        long totalPurchase = eventosPorTipo.getOrDefault("PURCHASE", 0L);
                        double conversion = totalAddCart == 0 ? 0.0 : (100.0 * totalPurchase / totalAddCart);

                        StringBuilder sb = new StringBuilder();
                        sb.append("\n================ METRICAS (ultimos 30s) ================\n");
                        sb.append(String.format("Eventos totales: %d  (%.2f eventos/seg)%n", totalEventos, eventosPorSegundo));
                        sb.append(String.format("Usuarios activos (distintos): %d%n", usuariosActivos.size()));
                        sb.append("Eventos por tipo: ").append(eventosPorTipo).append("\n");
                        sb.append(String.format("Conversion (PURCHASE / ADD_CART): %.1f%%%n", conversion));

                        sb.append("Top productos vistos: ").append(top(vistasPorProducto, 5)).append("\n");
                        sb.append("Top productos comprados: ").append(top(comprasPorProducto, 5)).append("\n");
                        sb.append("Compras por ciudad: ").append(comprasPorCiudad).append("\n");
                        sb.append("==========================================================\n");

                        out.collect(sb.toString());
                    }

                    private String top(Map<String, Long> mapa, int n) {
                        return mapa.entrySet().stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(n)
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .collect(Collectors.joining(", ", "[", "]"));
                    }
                })
                .print();

        env.execute("Audiencias Digitales - Metricas generales");
    }
}
