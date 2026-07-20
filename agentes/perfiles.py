"""
Perfiles de comportamiento de los 8 agentes (clientes virtuales).
Cada perfil define parámetros que el motor del agente (agente.py) usa para
decidir qué eventos generar, con qué probabilidad, y en qué horario.

No se usa ningún modelo de lenguaje aqui: el comportamiento "autonomo" surge
de reglas y probabilidades por perfil, tal como pide el enunciado.
"""

from dataclasses import dataclass, field
from typing import Optional, Tuple


@dataclass
class PerfilAgente:
    nombre: str
    descripcion: str

    # Cuantos productos suele consultar antes de decidir algo (min, max)
    productos_a_ver: Tuple[int, int]

    # Probabilidad de terminar comprando en una sesion (0.0 a 1.0)
    prob_compra: float

    # Probabilidad de agregar al carrito por cada producto visto
    prob_agregar_carrito: float

    # Si es True, el agente puede quitar productos del carrito y repetir
    indeciso: bool = False

    # Rango de valor de las compras que realiza (min, max) en soles
    valor_compra: Tuple[float, float] = (50, 800)

    # Horas del dia en que esta activo. None = activo todo el dia.
    # Ejemplo: (22, 5) significa activo de 10pm a 5am (cruza medianoche)
    horario_activo: Optional[Tuple[int, int]] = None

    # Tiempo de "pensado" entre eventos, en segundos (min, max)
    tiempo_entre_eventos: Tuple[float, float] = (1.0, 4.0)

    # Multiplicador de frecuencia de sesiones (mayor = mas activo/frecuente)
    frecuencia_sesiones: float = 1.0


PERFILES = {

    "COMPRADOR_COMPULSIVO": PerfilAgente(
        nombre="Comprador compulsivo",
        descripcion="Compra rapido, consulta pocos productos, alta probabilidad de compra, poco tiempo de navegacion.",
        productos_a_ver=(1, 2),
        prob_compra=0.75,
        prob_agregar_carrito=0.6,
        valor_compra=(80, 500),
        tiempo_entre_eventos=(0.5, 1.5),
        frecuencia_sesiones=1.3,
    ),

    "COMPARADOR": PerfilAgente(
        nombre="Comparador",
        descripcion="Consulta muchos productos, compara precios, compra ocasionalmente.",
        productos_a_ver=(5, 12),
        prob_compra=0.25,
        prob_agregar_carrito=0.3,
        valor_compra=(100, 600),
        tiempo_entre_eventos=(1.5, 3.5),
        frecuencia_sesiones=1.0,
    ),

    "COMPRADOR_NOCTURNO": PerfilAgente(
        nombre="Comprador nocturno",
        descripcion="Solo genera actividad durante la noche.",
        productos_a_ver=(2, 6),
        prob_compra=0.4,
        prob_agregar_carrito=0.4,
        valor_compra=(50, 700),
        horario_activo=(22, 5),
        tiempo_entre_eventos=(1.0, 3.0),
        frecuencia_sesiones=0.9,
    ),

    "CLIENTE_PREMIUM": PerfilAgente(
        nombre="Cliente Premium",
        descripcion="Compra pocas veces, pero de alto valor.",
        productos_a_ver=(2, 5),
        prob_compra=0.35,
        prob_agregar_carrito=0.5,
        valor_compra=(1500, 6000),
        tiempo_entre_eventos=(2.0, 5.0),
        frecuencia_sesiones=0.4,
    ),

    "CLIENTE_FRECUENTE": PerfilAgente(
        nombre="Cliente Frecuente",
        descripcion="Realiza compras constantemente.",
        productos_a_ver=(2, 5),
        prob_compra=0.55,
        prob_agregar_carrito=0.5,
        valor_compra=(60, 400),
        tiempo_entre_eventos=(1.0, 2.5),
        frecuencia_sesiones=1.8,
    ),

    "USUARIO_EXPLORADOR": PerfilAgente(
        nombre="Usuario Explorador",
        descripcion="Navega mucho, nunca compra.",
        productos_a_ver=(8, 20),
        prob_compra=0.0,
        prob_agregar_carrito=0.1,
        tiempo_entre_eventos=(1.0, 3.0),
        frecuencia_sesiones=1.1,
    ),

    "CLIENTE_INDECISO": PerfilAgente(
        nombre="Cliente Indeciso",
        descripcion="Agrega productos al carrito, los elimina y repite el proceso varias veces.",
        productos_a_ver=(3, 8),
        prob_compra=0.15,
        prob_agregar_carrito=0.7,
        indeciso=True,
        valor_compra=(50, 300),
        tiempo_entre_eventos=(1.0, 3.0),
        frecuencia_sesiones=1.0,
    ),

    "CLIENTE_ESTACIONAL": PerfilAgente(
        nombre="Cliente Estacional",
        descripcion="Su comportamiento cambia segun el evento empresarial activo (Navidad, Cyber Monday, etc).",
        productos_a_ver=(3, 8),
        prob_compra=0.2,   # se ajusta dinamicamente segun el escenario activo
        prob_agregar_carrito=0.3,
        valor_compra=(80, 500),
        tiempo_entre_eventos=(1.0, 3.0),
        frecuencia_sesiones=1.0,
    ),
}
