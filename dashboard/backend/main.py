"""
Backend del Dashboard - Trabajo Unidad II Big Data 2026A

Consume los topicos de salida que publica Flink (metricas y audiencias)
y retransmite cada mensaje nuevo, en tiempo real, a todos los navegadores
conectados via WebSocket. No usa base de datos: es un puente Kafka -> WS.
"""

import asyncio
import json
import threading
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_METRICAS = 'audiencias-metricas-output'
TOPIC_AUDIENCIAS = 'audiencias-resultado-output'

app = FastAPI(title="Dashboard Audiencias Digitales - BigData 2026A")

clientes_conectados: set = set()
cola_mensajes: asyncio.Queue | None = None
loop_principal: asyncio.AbstractEventLoop | None = None


def consumir_kafka():
    """Corre en un hilo aparte (bloqueante) leyendo ambos topicos de salida."""
    consumer = KafkaConsumer(
        TOPIC_METRICAS, TOPIC_AUDIENCIAS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        group_id='dashboard-backend',
        value_deserializer=lambda v: v.decode('utf-8'),
    )
    print(f"[dashboard-backend] escuchando topicos: {TOPIC_METRICAS}, {TOPIC_AUDIENCIAS}")
    for mensaje in consumer:
        tipo = 'metrica' if mensaje.topic == TOPIC_METRICAS else 'audiencia'
        payload = json.dumps({'tipo': tipo, 'data': json.loads(mensaje.value)})
        if loop_principal is not None and cola_mensajes is not None:
            loop_principal.call_soon_threadsafe(cola_mensajes.put_nowait, payload)


async def difundir_mensajes():
    """Toma mensajes de la cola y los envia a todos los clientes WebSocket conectados."""
    while True:
        mensaje = await cola_mensajes.get()
        desconectados = []
        for ws in clientes_conectados:
            try:
                await ws.send_text(mensaje)
            except Exception:
                desconectados.append(ws)
        for ws in desconectados:
            clientes_conectados.discard(ws)


@app.on_event("startup")
async def iniciar():
    global cola_mensajes, loop_principal
    loop_principal = asyncio.get_event_loop()
    cola_mensajes = asyncio.Queue()
    threading.Thread(target=consumir_kafka, daemon=True).start()
    asyncio.create_task(difundir_mensajes())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clientes_conectados.add(websocket)
    print(f"[dashboard-backend] cliente conectado. Total: {len(clientes_conectados)}")
    try:
        while True:
            await websocket.receive_text()  # mantiene viva la conexion
    except WebSocketDisconnect:
        clientes_conectados.discard(websocket)
        print(f"[dashboard-backend] cliente desconectado. Total: {len(clientes_conectados)}")


FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(FRONTEND_DIR / "index.html"))