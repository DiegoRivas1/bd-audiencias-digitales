"""
Motor de un agente individual. Cada instancia de Agente corre en su propio
hilo, simulando sesiones de navegacion continuas segun su perfil de
comportamiento y el escenario de negocio activo.
"""

import random
import time
import threading
from datetime import datetime

from perfiles import PerfilAgente
from escenarios import Escenario
import config


class Agente(threading.Thread):

    def __init__(self, agente_id: str, tipo_perfil: str, perfil: PerfilAgente,
                 escenario: Escenario, productor, detener_evento: threading.Event):
        super().__init__(daemon=True)
        self.agente_id = agente_id
        self.tipo_perfil = tipo_perfil
        self.perfil = perfil
        self.escenario = escenario
        self.productor = productor
        self.detener = detener_evento
        self.user_id = f"USR{random.randint(100, 999)}{agente_id[-2:]}"
        self.carrito = []

    # ---------- utilidades de comportamiento ----------

    def _esta_en_horario_activo(self) -> bool:
        if self.perfil.horario_activo is None:
            return True
        inicio, fin = self.perfil.horario_activo
        hora_actual = datetime.now().hour
        if inicio <= fin:
            return inicio <= hora_actual < fin
        # rango que cruza medianoche, ej (22, 5)
        return hora_actual >= inicio or hora_actual < fin

    def _esperar(self):
        lo, hi = self.perfil.tiempo_entre_eventos
        time.sleep(random.uniform(lo, hi))

    def _producto_aleatorio(self):
        nombre, categoria, precio_base = random.choice(config.CATALOGO)
        precio = round(precio_base * random.uniform(0.95, 1.08), 2)
        return nombre, categoria, precio

    def _emitir(self, topic: str, evento: dict):
        evento['timestamp'] = datetime.now().isoformat(timespec='seconds')
        evento['user_id'] = self.user_id
        evento['company'] = config.COMPANY
        evento['agent_id'] = self.agente_id
        evento['agent_type'] = self.tipo_perfil
        self.productor.send(topic, value=evento)

    # ---------- sesion de navegacion ----------

    def _sesion(self):
        """Genera una sesion completa de eventos para este agente."""

        self._emitir(config.TOPIC_USER_EVENTS, {
            'event': 'LOGIN',
            'product': None,
            'category': None,
            'city': random.choice(config.CIUDADES),
        })
        self._esperar()

        min_prod, max_prod = self.perfil.productos_a_ver
        num_productos = random.randint(min_prod, max_prod)

        prob_compra = min(self.perfil.prob_compra * self.escenario.mult_prob_compra, 0.95)
        prob_carrito = self.perfil.prob_agregar_carrito

        # dar preferencia a categorias destacadas del escenario, si aplica
        for _ in range(num_productos):
            if self.escenario.categorias_destacadas and random.random() < 0.5:
                candidatos = [p for p in config.CATALOGO if p[1] in self.escenario.categorias_destacadas]
                nombre, categoria, precio_base = random.choice(candidatos) if candidatos else random.choice(config.CATALOGO)
                precio = round(precio_base * random.uniform(0.95, 1.08), 2)
            else:
                nombre, categoria, precio = self._producto_aleatorio()

            evento_tipo = 'SEARCH' if random.random() < 0.4 else 'VIEW_PRODUCT'
            self._emitir(config.TOPIC_USER_EVENTS, {
                'event': evento_tipo,
                'product': nombre,
                'category': categoria,
                'city': random.choice(config.CIUDADES),
                'price': precio,
            })
            self._esperar()

            if random.random() < prob_carrito:
                self.carrito.append((nombre, categoria, precio))
                self._emitir(config.TOPIC_USER_EVENTS, {
                    'event': 'ADD_CART',
                    'product': nombre,
                    'category': categoria,
                    'city': random.choice(config.CIUDADES),
                    'price': precio,
                })
                self._esperar()

                # Cliente Indeciso: a veces quita el producto y sigue dando vueltas
                if self.perfil.indeciso and random.random() < 0.5:
                    self.carrito.pop()
                    self._emitir(config.TOPIC_USER_EVENTS, {
                        'event': 'REMOVE_CART',
                        'product': nombre,
                        'category': categoria,
                        'city': random.choice(config.CIUDADES),
                        'price': precio,
                    })
                    self._esperar()

        # decision final de compra
        if self.carrito and random.random() < prob_compra:
            lo, hi = self.perfil.valor_compra
            valor_objetivo = random.uniform(lo, hi) * self.escenario.mult_valor_compra
            producto_compra = random.choice(self.carrito)
            nombre, categoria, _precio = producto_compra

            exito_pago = random.random() < 0.92  # 8% de pagos rechazados

            self._emitir(config.TOPIC_PURCHASE_EVENTS, {
                'event': 'PURCHASE' if exito_pago else 'PAYMENT_FAILED',
                'product': nombre,
                'category': categoria,
                'city': random.choice(config.CIUDADES),
                'price': round(valor_objetivo, 2),
            })
            self._esperar()
            self.carrito.clear()

        self._emitir(config.TOPIC_USER_EVENTS, {
            'event': 'LOGOUT',
            'product': None,
            'category': None,
            'city': random.choice(config.CIUDADES),
        })

    # ---------- loop principal del hilo ----------

    def run(self):
        while not self.detener.is_set():
            if self._esta_en_horario_activo():
                try:
                    self._sesion()
                except Exception as exc:
                    print(f"[{self.agente_id}] error en sesion: {exc}")

                pausa_base = random.uniform(3, 10)
                pausa = pausa_base / max(self.perfil.frecuencia_sesiones * self.escenario.mult_frecuencia_sesiones, 0.1)
                time.sleep(pausa)
            else:
                # fuera de horario activo: revisa cada rato si ya entro en su ventana
                time.sleep(30)
