"""
Escenarios de negocio. Cada escenario aplica multiplicadores globales sobre
el comportamiento base de los agentes (definido en perfiles.py), simulando
como cambia la actividad de los usuarios durante fechas comerciales clave.

El Cliente Estacional (perfiles.py) es el que mas reacciona a estos cambios,
pero los multiplicadores se aplican a TODOS los agentes para reflejar un
efecto de campaña a nivel de toda la plataforma.
"""

from dataclasses import dataclass


@dataclass
class Escenario:
    nombre: str
    descripcion: str
    mult_prob_compra: float       # multiplica la probabilidad de compra
    mult_frecuencia_sesiones: float  # multiplica cuan seguido inician sesion
    mult_valor_compra: float      # multiplica el valor promedio de compra
    categorias_destacadas: list   # categorias con mas trafico en este escenario


ESCENARIOS = {

    "NORMAL": Escenario(
        nombre="Dia normal",
        descripcion="Comportamiento base, sin campañas activas.",
        mult_prob_compra=1.0,
        mult_frecuencia_sesiones=1.0,
        mult_valor_compra=1.0,
        categorias_destacadas=[],
    ),

    "NAVIDAD": Escenario(
        nombre="Navidad",
        descripcion="Alta intencion de compra de regalos, tickets moderados-altos.",
        mult_prob_compra=1.8,
        mult_frecuencia_sesiones=1.6,
        mult_valor_compra=1.3,
        categorias_destacadas=["Electronics", "Toys", "Furniture"],
    ),

    "CYBER_MONDAY": Escenario(
        nombre="Cyber Monday",
        descripcion="Pico masivo de trafico y compras impulsivas por descuentos.",
        mult_prob_compra=2.2,
        mult_frecuencia_sesiones=2.5,
        mult_valor_compra=0.9,   # tickets mas bajos pero muchas mas compras
        categorias_destacadas=["Electronics", "Accessories"],
    ),

    "BLACK_FRIDAY": Escenario(
        nombre="Black Friday",
        descripcion="Similar a Cyber Monday, fuerte pico de trafico y conversion.",
        mult_prob_compra=2.0,
        mult_frecuencia_sesiones=2.3,
        mult_valor_compra=1.0,
        categorias_destacadas=["Electronics", "Accessories", "Furniture"],
    ),

    "DIA_DEL_PADRE": Escenario(
        nombre="Dia del Padre",
        descripcion="Aumento moderado en categorias especificas.",
        mult_prob_compra=1.4,
        mult_frecuencia_sesiones=1.3,
        mult_valor_compra=1.1,
        categorias_destacadas=["Accessories", "Electronics"],
    ),

    "FIESTAS_PATRIAS": Escenario(
        nombre="Fiestas Patrias",
        descripcion="Campaña nacional con aumento general de trafico.",
        mult_prob_compra=1.5,
        mult_frecuencia_sesiones=1.4,
        mult_valor_compra=1.15,
        categorias_destacadas=["Furniture", "Electronics"],
    ),

    "CAMPANA_ESCOLAR": Escenario(
        nombre="Campaña Escolar",
        descripcion="Aumento en utiles y tecnologia basica, tickets bajos-medios.",
        mult_prob_compra=1.3,
        mult_frecuencia_sesiones=1.2,
        mult_valor_compra=0.85,
        categorias_destacadas=["Accessories", "Electronics"],
    ),
}


def obtener_escenario(nombre: str) -> Escenario:
    clave = nombre.strip().upper().replace(" ", "_")
    if clave not in ESCENARIOS:
        disponibles = ", ".join(ESCENARIOS.keys())
        raise ValueError(f"Escenario '{nombre}' no existe. Disponibles: {disponibles}")
    return ESCENARIOS[clave]
