# Plataforma Inteligente para Simulación y Análisis de Audiencias Digitales en Tiempo Real

**Curso:** BigData 2026A 
**Trabajo:** Unidad III — Arquitectura Orientada a Eventos (Agentes → Kafka → Flink → Audiencias → Dashboard)

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

## Estado del proyecto

- [x] Diseño de arquitectura orientada a eventos
- [x] Simulador de agentes (8 perfiles)
- [ ] Productores publicando a Kafka
- [ ] Configuración de Kafka (topics/particiones)
- [ ] Procesamiento con Apache Flink
- [ ] Construcción de audiencias digitales
- [ ] Dashboard en tiempo real
- [ ] Ejecución bajo distintos escenarios (Navidad, Cyber Monday, Fiestas Patrias, etc.)
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
