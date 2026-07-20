package com.epcc.audiencias;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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
 * Job unico que alimenta el dashboard: calcula metricas por ventana y
 * clasifica audiencias por usuario, publicando ambos resultados como JSON
 * hacia topicos Kafka de salida que consume el backend del dashboard.
 */
public class MainDashboardPipeline {

    private static final Time VENTANA = Time.seconds(15);
    public static final String TOPIC_METRICAS_OUT = "audiencias-metricas-output";
    public static final String TOPIC_AUDIENCIAS_OUT = "audiencias-resultado-output";

    private static final ObjectMapper MAPPER_AUDIENCIAS = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Evento> eventos = FuenteEventos.obtenerStream(env, "flink-audiencias-dashboard");

        // ---------- rama 1: metricas por ventana ----------
        DataStream<String> metricasJson = eventos
                .windowAll(TumblingProcessingTimeWindows.of(VENTANA))
                .process(new ProcessAllWindowFunction<Evento, String, TimeWindow>() {

                    private transient ObjectMapper mapper;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        mapper = new ObjectMapper();
                    }

                    @Override
                    public void process(Context context, Iterable<Evento> elementos, Collector<String> out) throws Exception {
                        List<Evento> lista = new ArrayList<>();
                        elementos.forEach(lista::add);

                        int totalEventos = lista.size();
                        double eventosPorSegundo = totalEventos / (VENTANA.toMilliseconds() / 1000.0);

                        Set<String> usuariosActivos = lista.stream().map(e -> e.user_id).collect(Collectors.toSet());

                        Map<String, Long> eventosPorTipo = lista.stream()
                                .collect(Collectors.groupingBy(e -> e.event, LinkedHashMap::new, Collectors.counting()));

                        Map<String, Long> vistasPorProducto = ordenarTop(lista.stream()
                                .filter(e -> "VIEW_PRODUCT".equals(e.event) && e.product != null)
                                .collect(Collectors.groupingBy(e -> e.product, Collectors.counting())), 5);

                        Map<String, Long> comprasPorProducto = ordenarTop(lista.stream()
                                .filter(e -> "PURCHASE".equals(e.event) && e.product != null)
                                .collect(Collectors.groupingBy(e -> e.product, Collectors.counting())), 5);

                        Map<String, Long> comprasPorCiudad = lista.stream()
                                .filter(e -> "PURCHASE".equals(e.event))
                                .collect(Collectors.groupingBy(e -> e.city, LinkedHashMap::new, Collectors.counting()));

                        long totalAddCart = eventosPorTipo.getOrDefault("ADD_CART", 0L);
                        long totalPurchase = eventosPorTipo.getOrDefault("PURCHASE", 0L);
                        double conversion = totalAddCart == 0 ? 0.0 : (100.0 * totalPurchase / totalAddCart);

                        Map<String, Object> salida = new LinkedHashMap<>();
                        salida.put("timestamp", java.time.LocalDateTime.now().toString());
                        salida.put("eventosTotales", totalEventos);
                        salida.put("eventosPorSegundo", Math.round(eventosPorSegundo * 100.0) / 100.0);
                        salida.put("usuariosActivos", usuariosActivos.size());
                        salida.put("eventosPorTipo", eventosPorTipo);
                        salida.put("conversion", Math.round(conversion * 10.0) / 10.0);
                        salida.put("topProductosVistos", vistasPorProducto);
                        salida.put("topProductosComprados", comprasPorProducto);
                        salida.put("comprasPorCiudad", comprasPorCiudad);

                        out.collect(mapper.writeValueAsString(salida));
                    }

                    private Map<String, Long> ordenarTop(Map<String, Long> mapa, int n) {
                        Map<String, Long> resultado = new LinkedHashMap<>();
                        mapa.entrySet().stream()
                                .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
                                .limit(n)
                                .forEach(e -> resultado.put(e.getKey(), e.getValue()));
                        return resultado;
                    }
                });

        metricasJson.sinkTo(construirSink(TOPIC_METRICAS_OUT));

        // ---------- rama 2: clasificacion de audiencias ----------
        DataStream<String> audienciasJson = eventos
                .keyBy(e -> e.user_id)
                .process(new ClasificadorAudiencias())
                .map(resultado -> MAPPER_AUDIENCIAS.writeValueAsString(resultado));

        audienciasJson.sinkTo(construirSink(TOPIC_AUDIENCIAS_OUT));

        // impresion en consola tambien, util para depurar mientras se prueba el dashboard
        metricasJson.print("METRICAS");
        audienciasJson.print("AUDIENCIA");

        env.execute("Audiencias Digitales - Pipeline para Dashboard");
    }

    private static KafkaSink<String> construirSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(FuenteEventos.BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }
}