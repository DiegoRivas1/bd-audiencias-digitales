"""
Configuración general del simulador: conexión a Kafka, tópicos, catálogo
de productos y cuántos agentes de cada perfil se van a lanzar.
"""

# --- Kafka ---
BOOTSTRAP_SERVERS = ['localhost:9092']  # cambia si el productor corre fuera de la EC2

TOPIC_USER_EVENTS = 'user-events'
TOPIC_PURCHASE_EVENTS = 'purchase-events'

# --- Catálogo de productos: (nombre, categoria, precio_base) ---
CATALOGO = [
    ('Laptop Lenovo', 'Electronics', 3200),
    ('Laptop Dell', 'Electronics', 3500),
    ('Mouse Logitech', 'Accessories', 45),
    ('Teclado Mecanico', 'Accessories', 180),
    ('Monitor LG 24"', 'Electronics', 650),
    ('Audifonos Sony', 'Accessories', 220),
    ('Silla Gamer', 'Furniture', 890),
    ('Escritorio Madera', 'Furniture', 450),
    ('Mochila Escolar', 'Accessories', 90),
    ('Tablet Samsung', 'Electronics', 1200),
    ('Smartwatch Xiaomi', 'Electronics', 350),
    ('Impresora HP', 'Electronics', 680),
]

CIUDADES = ['Arequipa', 'Lima', 'Cusco', 'Trujillo', 'Piura']

COMPANY = 'Retail'

# --- Cuántos agentes lanzar por cada perfil ---
# Ajusta estos números según cuánta carga quieras simular.
AGENTES_POR_PERFIL = {
    "COMPRADOR_COMPULSIVO": 3,
    "COMPARADOR": 3,
    "COMPRADOR_NOCTURNO": 2,
    "CLIENTE_PREMIUM": 2,
    "CLIENTE_FRECUENTE": 3,
    "USUARIO_EXPLORADOR": 2,
    "CLIENTE_INDECISO": 2,
    "CLIENTE_ESTACIONAL": 3,
}
