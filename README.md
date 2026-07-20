# Plataforma Inteligente para Simulación y Análisis de Audiencias Digitales en Tiempo Real

**Curso:** BigData 2026A — UNSA-EPCC
**Trabajo:** Unidad II — Arquitectura Orientada a Eventos (Agentes → Kafka → Flink → Audiencias → Dashboard)

## Arquitectura

```
Agentes simulados (Python)
        │  publican eventos JSON
        ▼
   Apache Kafka  (topics: user-events, purchase-events)
        │  consumido continuamente
        ▼
   Apache Flink  (filtrado, enriquecimiento, ventanas, clasificación de audiencias)
        │  resultados / métricas
        ▼
   Dashboard en tiempo real (backend + frontend)
```

## Estructura del repositorio

```
agentes/            Simulador de agentes (8 perfiles) + productor Kafka
flink-audiendas/    Proyecto Maven Flink: procesamiento, audiencias, métricas
dashboard/          Backend (API/WebSocket) + frontend del dashboard en tiempo real
infra/              Scripts de instalación y despliegue en EC2 (Kafka, topics)
docs/               Documentación de arquitectura y capturas para el entregable PDF
```

## Notas de operación

### Kafka pierde los datos al reiniciar la EC2

Por defecto, Kafka guarda sus logs en `/tmp/kraft-combined-logs`, y `/tmp` se
borra cada vez que se reinicia la instancia. Esto obliga a reformatear el
storage (`kafka-storage.sh format`) cada vez que se detiene/enciende la EC2.

**Solución aplicada:** cambiar `log.dirs` en `config/server.properties` a una
ruta persistente antes de formatear por primera vez:

```bash
sed -i 's#^log.dirs=.*#log.dirs=/home/ubuntu/kafka-data#' config/server.properties
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

Si la instancia no tiene una Elastic IP, igual hay que actualizar
`advertised.listeners` con la IP publica nueva cada vez que se reinicie.

## Estado del proyecto

- [x] Diseño de arquitectura orientada a eventos
- [x] Simulador de agentes (8 perfiles)
- [x] Productores publicando a Kafka
- [x] Configuración de Kafka (topics/particiones)
- [x] Procesamiento con Apache Flink (métricas + audiencias)
- [x] Construcción de audiencias digitales
- [ ] Dashboard en tiempo real
- [ ] Ejecución bajo distintos escenarios (comparativa documentada)
- [ ] (Extra) Enriquecimiento de audiencias con LLM / API

## Cómo correr (resumen)

Ver instrucciones detalladas en cada subcarpeta. Resumen rápido:

```bash
# 1. Kafka (en la EC2)
bash infra/setup_kafka.sh
bash infra/crear_topics.sh

# 2. Simulador de agentes
cd agentes
pip install -r requirements.txt --break-system-packages
python3 productor_agentes.py --escenario normal

# 3. Flink
cd flink-audiencias
mvn clean package
java -cp target/flink-audiencias-1.0.jar com.epcc.audiencias.MainAudiencias

# 4. Dashboard
cd dashboard/backend
# (instrucciones pendientes en dashboard/backend/README.md)
```