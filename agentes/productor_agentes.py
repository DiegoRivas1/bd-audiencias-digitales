"""
Productor de agentes - Trabajo Unidad II Big Data 2026A
Lanza el simulador completo: N agentes por cada uno de los 8 perfiles,
cada uno corriendo en su propio hilo y publicando eventos JSON a Kafka
(topics: user-events, purchase-events) segun su comportamiento y el
escenario de negocio activo.

Uso:
    python3 productor_agentes.py --escenario normal
    python3 productor_agentes.py --escenario cyber_monday
    python3 productor_agentes.py --escenario navidad --duracion 300
"""

import argparse
import json
import signal
import threading
import time

from kafka import KafkaProducer

import config
from perfiles import PERFILES
from escenarios import obtener_escenario
from agente import Agente


def parse_args():
    parser = argparse.ArgumentParser(description="Simulador de agentes de audiencias digitales")
    parser.add_argument(
        '--escenario', default='normal',
        help="Escenario de negocio activo: normal, navidad, cyber_monday, black_friday, "
             "dia_del_padre, fiestas_patrias, campana_escolar"
    )
    parser.add_argument(
        '--duracion', type=int, default=0,
        help="Duracion en segundos de la simulacion. 0 = correr indefinidamente (Ctrl+C para detener)."
    )
    return parser.parse_args()


def main():
    args = parse_args()
    escenario = obtener_escenario(args.escenario)

    print(f"Escenario activo: {escenario.nombre} - {escenario.descripcion}")
    print(f"Conectando a Kafka en {config.BOOTSTRAP_SERVERS} ...")

    productor = KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    detener_evento = threading.Event()
    agentes = []
    contador_global = 0

    for tipo_perfil, cantidad in config.AGENTES_POR_PERFIL.items():
        perfil = PERFILES[tipo_perfil]
        for i in range(cantidad):
            contador_global += 1
            agente_id = f"{tipo_perfil}_{i+1:02d}"
            agente = Agente(
                agente_id=agente_id,
                tipo_perfil=tipo_perfil,
                perfil=perfil,
                escenario=escenario,
                productor=productor,
                detener_evento=detener_evento,
            )
            agentes.append(agente)

    print(f"Lanzando {contador_global} agentes ({len(config.AGENTES_POR_PERFIL)} perfiles distintos)...")
    for agente in agentes:
        agente.start()

    def manejar_salida(sig, frame):
        print("\nDeteniendo simulacion (esperando a que los agentes terminen su sesion actual)...")
        detener_evento.set()

    signal.signal(signal.SIGINT, manejar_salida)

    inicio = time.time()
    try:
        while not detener_evento.is_set():
            time.sleep(1)
            if args.duracion and (time.time() - inicio) >= args.duracion:
                print(f"\nDuracion de {args.duracion}s alcanzada, deteniendo simulacion...")
                detener_evento.set()
    except KeyboardInterrupt:
        detener_evento.set()

    for agente in agentes:
        agente.join(timeout=5)

    productor.flush()
    productor.close()
    print("Simulacion finalizada.")


if __name__ == '__main__':
    main()
