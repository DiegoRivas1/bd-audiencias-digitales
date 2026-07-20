package com.epcc.audiencias;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Consume ambos topicos, agrupa por usuario, y aplica el clasificador de
 * audiencias digitales, imprimiendo cada resultado detectado en tiempo real.
 */
public class MainAudiencias {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Evento> eventos = FuenteEventos.obtenerStream(env, "flink-audiencias-clasificador");

        DataStream<AudienciaResultado> audiencias = eventos
                .keyBy(e -> e.user_id)
                .process(new ClasificadorAudiencias());

        audiencias.map(a -> "[AUDIENCIA DETECTADA] " + a.toString()).print();

        env.execute("Audiencias Digitales - Clasificador por usuario");
    }
}
