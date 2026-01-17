"""
Configuración centralizada del proyecto.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno (si existe .env)
load_dotenv()

# Rutas del proyecto
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Configuración de PostgreSQL
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "ecommerce_db"),
    "user": os.getenv("POSTGRES_USER", "ecommerce"),
    "password": os.getenv("POSTGRES_PASSWORD", "ecommerce123"),
}

# Configuración de Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topics": {
        "clicks": os.getenv("KAFKA_TOPIC_CLICKS", "user_clicks"),
        "cart": os.getenv("KAFKA_TOPIC_CART", "cart_events"),
        "purchases": os.getenv("KAFKA_TOPIC_PURCHASES", "purchases"),
    }
}

# Configuración de Spark
SPARK_CONFIG = {
    "master": os.getenv("SPARK_MASTER", "local[*]"),
    "app_name": "TFM-Ecommerce-Analytics",
}


def ensure_directories():
    """Crea los directorios necesarios si no existen."""
    for directory in [RAW_DATA_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        directory.mkdir(parents=True, exist_ok=True)