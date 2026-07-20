package com.epcc.audiencias;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Construye el stream de eventos unificado, consumiendo simultaneamente
 * audiencias-user-events y audiencias-purchase-events.
 */
public class FuenteEventos {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC_USER = "audiencias-user-events";
    public static final String TOPIC_PURCHASE = "audiencias-purchase-events";

    public static DataStream<Evento> obtenerStream(StreamExecutionEnvironment env, String groupId) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC_USER, TOPIC_PURCHASE)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source - eventos audiencias");

        ObjectMapper mapper = new ObjectMapper();
        return raw.map(json -> mapper.readValue(json, Evento.class));
    }
}
