# Arquitectura  Plataforma de Audiencias Digitales en Tiempo Real

## Flujo general

```
[Agentes simulados]  --publican-->  [Apache Kafka]  --consume-->  [Apache Flink]  --resultados-->  [Dashboard]
```

## 1. Agentes (capa de simulación)

8 perfiles de comportamiento (ver `agentes/perfiles.py`), cada uno implementado
como un hilo independiente (`agentes/agente.py`) que simula sesiones de
navegación reales: login → ver/buscar productos → (agregar/quitar carrito) →
compra o abandono → logout.

El comportamiento de cada agente se ve afectado por un **escenario de negocio**
activo (`agentes/escenarios.py`) que ajusta multiplicadores de:
- probabilidad de compra
- frecuencia de sesiones
- valor promedio de compra
- categorías con más tráfico

## 2. Kafka (capa de ingesta)

Topics:
- `user-events`: LOGIN, SEARCH, VIEW_PRODUCT, ADD_CART, REMOVE_CART, LOGOUT
- `purchase-events`: PURCHASE, PAYMENT_FAILED

6 particiones cada uno (permite paralelismo real en Flink).

## 3. Flink (capa de procesamiento)

Pendiente de implementar en `flink-audiencias/`. Debe:
- Consumir ambos tópicos
- Enriquecer eventos con atributos derivados (hora, día, fin de semana)
- Calcular métricas en ventanas de tiempo (eventos/seg, usuarios activos, etc.)
- Clasificar usuarios en audiencias según reglas de negocio (ver sección Audiencias)
- Emitir resultados hacia el dashboard (WebSocket, tópico de salida, o base de datos)

## 4. Audiencias digitales (reglas propuestas)

| Audiencia | Regla |
|---|---|
| Alta intención de compra | ADD_CART sin PURCHASE en los últimos N minutos |
| Clientes frecuentes | ≥3 PURCHASE en la ventana de análisis |
| Clientes Premium | Compra con valor > umbral (ej. S/1500) |
| Abandono de carrito | ADD_CART sin PURCHASE al cierre de sesión (LOGOUT) |
| Interesados en tecnología | ≥60% de sus VIEW_PRODUCT en categoría Electronics |
| Riesgo de abandono | Sesiones activas con muchos VIEW_PRODUCT y 0 ADD_CART |

## 5. Dashboard (capa de visualización)

Pendiente de implementar en `dashboard/`. Debe visualizar en tiempo real:
usuarios activos, eventos/seg, eventos por tipo, audiencias detectadas,
productos más vistos/comprados, compras por región, tendencias temporales,
conversión de compras, alertas.
