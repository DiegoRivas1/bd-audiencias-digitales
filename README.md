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

## Cómo correr el proyecto completo (guía paso a paso)

### 0. Requisitos en la instancia EC2 (Ubuntu 24.04)

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk maven python3-pip tmux
java -version
mvn -version
```

### 1. Instalar y configurar Apache Kafka (modo KRaft)

```bash
cd ~
wget https://downloads.apache.org/kafka/4.2.1/kafka_2.13-4.2.1.tgz
tar -xzf kafka_2.13-4.2.1.tgz
mv kafka_2.13-4.2.1 kafka
cd kafka
```

**Importante:** cambiar `log.dirs` a una ruta persistente ANTES de formatear
(ver sección "Notas de operación" más abajo evita tener que reformatear
cada vez que se reinicia la EC2):

```bash
sed -i 's#^log.dirs=.*#log.dirs=/home/ubuntu/kafka-data#' config/server.properties
```

Formatear el storage:
```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

Configurar los listeners con la IP pública de la instancia:
```bash
sed -i '/^listeners=/d; /^advertised.listeners=/d' config/server.properties
echo "listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093" >> config/server.properties
echo "advertised.listeners=PLAINTEXT://<TU_IP_PUBLICA>:9092" >> config/server.properties
```

Verificar que quedó limpio (debe mostrar solo 2 líneas):
```bash
grep -n "^listeners\|^advertised.listeners" config/server.properties
```

Levantar el broker (dejar corriendo en su propia terminal/ventana tmux):
```bash
bin/kafka-server-start.sh config/server.properties
```

### 2. Clonar el repositorio e instalar dependencias Python

```bash
cd ~
git clone https://github.com/DiegoRivas1/bd-audiencias-digitales.git
cd bd-audiencias-digitales
pip3 install kafka-python --break-system-packages
```

### 3. Crear los tópicos de Kafka

El script usa rutas relativas, así que se ejecuta desde `~/kafka`:
```bash
cd ~/kafka
bash ~/bd-audiencias-digitales/infra/crear_topics.sh
```

Esto crea 4 tópicos: `audiencias-user-events`, `audiencias-purchase-events`
(entrada, generados por los agentes) y `audiencias-metricas-output`,
`audiencias-resultado-output` (salida, generados por Flink para el dashboard).

*Nota: si el script da "Permission denied", ejecútalo con `bash script.sh`
en vez de `./script.sh`  git no siempre conserva el bit +x al subir archivos.*

### 4. Compilar y correr Apache Flink

```bash
cd ~/bd-audiencias-digitales/flink-audiencias
mvn clean package -DskipTests
```

Hay 3 programas ejecutables, cada uno con `java -cp target/flink-audiencias-1.0.jar com.epcc.audiencias.<Clase>`:

| Clase | Qué hace | Cuándo usarla |
|---|---|---|
| `MainMetricas` | Imprime métricas por consola cada 30s | Pruebas rápidas, sin dashboard |
| `MainAudiencias` | Imprime audiencias detectadas por consola | Pruebas rápidas, sin dashboard |
| `MainDashboardPipeline` | Calcula ambas cosas (ventanas de 15s) y las publica a Kafka para el dashboard | **Usar esta para correr el dashboard** |

```bash
java -cp target/flink-audiencias-1.0.jar com.epcc.audiencias.MainDashboardPipeline
```

### 5. Levantar el backend del dashboard

```bash
cd ~/bd-audiencias-digitales/dashboard/backend
pip3 install -r requirements.txt --break-system-packages
python3 -m uvicorn main:app --host 0.0.0.0 --port 3000
```

Abrir en el navegador: `http://<TU_IP_PUBLICA>:3000`

(el Security Group de la EC2 debe tener el puerto 3000 abierto)

### 6. Correr el simulador de agentes

```bash
cd ~/bd-audiencias-digitales/agentes
pip3 install -r requirements.txt --break-system-packages
python3 productor_agentes.py --escenario <ESCENARIO>
```

**Escenarios disponibles** (parámetro `--escenario`):

| Flag | Escenario | Efecto |
|---|---|---|
| `normal` | Día normal | Comportamiento base, sin campaña |
| `navidad` | Navidad | Alta compra, tickets moderados-altos |
| `cyber_monday` | Cyber Monday | Pico masivo de tráfico y compras impulsivas |
| `black_friday` | Black Friday | Similar a Cyber Monday |
| `dia_del_padre` | Día del Padre | Aumento moderado, categorías específicas |
| `fiestas_patrias` | Fiestas Patrias | Aumento general de tráfico |
| `campana_escolar` | Campaña Escolar | Tickets bajos-medios |

**Solo debe correr UNA instancia del simulador a la vez.** Si corres dos en
paralelo (por ejemplo, olvidaste cerrar la anterior), Flink mezcla los
eventos de ambos escenarios en la misma ventana de tiempo y las métricas
dejan de ser representativas de un escenario puro. Antes de cambiar de
escenario, verificar que no quede ningún proceso viejo corriendo:

```bash
ps aux | grep productor_agentes
# si aparece uno viejo:
kill <PID>
```

Para cambiar de escenario: `Ctrl+C` en la terminal del simulador y volver a
correr el comando con el flag distinto. **No hace falta reiniciar Kafka,
Flink ni el backend** el dashboard se actualiza solo en vivo.

### 7. Verificación manual opcional (sin dashboard)

Para ver los eventos crudos llegando a Kafka:
```bash
cd ~/kafka
bin/kafka-console-consumer.sh --topic audiencias-user-events --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic audiencias-purchase-events --bootstrap-server localhost:9092
```

### Recomendado: usar tmux para no perder los procesos

Con 4 procesos corriendo a la vez (Kafka, Flink, backend, simulador), si se
corta la sesión SSH se mueren todos. Usar `tmux` para evitarlo:

```bash
tmux new -s proyecto
# Ctrl+B, C -> crea una ventana nueva (una por proceso)
# Ctrl+B, <numero> -> cambia entre ventanas
# Ctrl+B, D -> se sale de tmux sin matar nada (los procesos siguen corriendo)
# Para volver a entrar despues de reconectar por SSH:
tmux attach -t proyecto
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
- [x] Dashboard en tiempo real
- [x] Ejecución bajo distintos escenarios (comparativa documentada)
- [ ] (Extra) Enriquecimiento de audiencias con LLM / API