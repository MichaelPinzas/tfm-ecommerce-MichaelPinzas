"""
Transformación Bronze → Silver
Limpieza, enriquecimiento y preparación de datos para análisis
"""

import pyarrow as pa
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, split, size, hour, dayofweek,
    date_format, to_date, year, month, dayofmonth
)
from pyspark.sql.window import Window
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session(app_name="Bronze to Silver"):
    """Crear sesión Spark con configuración optimizada para local"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_data(df):
    """
    Limpia datos eliminando registros inválidos

    Aplica:
    - Filtro de precios > 0
    - Eliminación de duplicados
    - Rellenar nulls en brand y category_code
    """
    logger.info("Iniciando limpieza de datos...")

    initial_count = df.count()
    logger.info(f"Registros iniciales: {initial_count:,}")

    # 1. Filtrar precios válidos (> 0)
    df = df.filter(col("price") > 0)
    valid_price_count = df.count()
    filtered = initial_count - valid_price_count
    logger.info(f"Registros filtrados por precio <= 0: {filtered:,}")

    # 2. Rellenar nulls en brand
    df = df.withColumn(
        "brand",
        when(col("brand").isNull(), lit("Unknown")).otherwise(col("brand"))
    )

    # 3. Rellenar nulls en category_code
    df = df.withColumn(
        "category_code",
        when(col("category_code").isNull(), lit("Unknown")).otherwise(col("category_code"))
    )

    # 4. Eliminar duplicados
    # Criterio: user_id + product_id + event_type + timestamp
    df = df.dropDuplicates([
        "user_id",
        "product_id",
        "event_type",
        "event_time"
    ])

    after_dedup_count = df.count()
    duplicates_removed = valid_price_count - after_dedup_count
    logger.info(f"Duplicados eliminados: {duplicates_removed:,}")

    final_count = df.count()
    logger.info(f"Registros después de limpieza: {final_count:,}")
    logger.info(
        f"Total registros eliminados: {initial_count - final_count:,} ({((initial_count - final_count) / initial_count * 100):.2f}%)")

    return df


def enrich_temporal(df):
    """
    Enriquece datos con columnas temporales derivadas de event_time

    Agrega:
    - hour: hora del día (0-23)
    - day_of_week: día de la semana (1=Monday, 7=Sunday)
    - is_weekend: boolean (Saturday/Sunday)
    - time_of_day: categorical (morning/afternoon/evening/night)
    """
    logger.info("Enriqueciendo con columnas temporales...")

    # TODO: Implementar enriquecimiento temporal

    return df


def parse_categories(df):
    """
    Parsea category_code en niveles jerárquicos

    Genera:
    - category_l1: primer nivel (ej: "electronics")
    - category_l2: segundo nivel (ej: "smartphone")
    - category_l3: tercer nivel (si existe)
    """
    logger.info("Parseando categorías jerárquicas...")

    # TODO: Implementar parsing de categorías

    return df


def calculate_conversion_stage(df):
    """
    Calcula la etapa del funnel de conversión para cada evento

    Lógica:
    - viewed_only: solo visualizó el producto
    - added_to_cart: agregó al carrito pero no compró
    - purchased: completó la compra
    """
    logger.info("Calculando conversion_stage...")

    # TODO: Implementar lógica de funnel

    return df


def main(bronze_path, silver_path, process_date=None):
    """
    Pipeline principal Bronze → Silver

    Args:
        bronze_path: ruta a datos Bronze
        silver_path: ruta de salida Silver
        process_date: fecha específica a procesar (YYYY-MM-DD) o None para todo
    """
    import pyarrow.parquet as pq
    import pandas as pd

    logger.info("=" * 80)
    logger.info("INICIANDO TRANSFORMACIÓN BRONZE → SILVER")
    logger.info("=" * 80)

    # Crear sesión Spark
    spark = get_spark_session()

    # Leer con PyArrow primero (evita el error de nanosegundos)
    logger.info(f"Leyendo datos desde: {bronze_path}")

    # Leer todo el dataset Bronze con PyArrow
    table = pq.read_table(str(bronze_path))

    # Convertir timestamp de nanosegundos a microsegundos
    # (compatible con Spark)
    table = table.cast(table.schema.set(
        table.schema.get_field_index('event_time'),
        pa.field('event_time', pa.timestamp('us', tz='UTC'))
    ))

    # Convertir a pandas DataFrame
    pdf = table.to_pandas()

    logger.info(f"Registros leídos: {len(pdf):,}")

    # Convertir pandas a Spark DataFrame
    df = spark.createDataFrame(pdf)

    # Aplicar transformaciones secuencialmente
    df = clean_data(df)
    df = enrich_temporal(df)
    df = parse_categories(df)
    df = calculate_conversion_stage(df)

    # Escribir a Silver manteniendo particionamiento
    logger.info(f"Escribiendo datos a: {silver_path}")
    df.write \
        .partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .parquet(str(silver_path))

    final_count = df.count()
    logger.info(f"Registros finales en Silver: {final_count:,}")
    logger.info("✅ Transformación completada exitosamente")

    spark.stop()


if __name__ == "__main__":
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"
    silver_path = project_root / "data" / "silver" / "events"

    # Ejecutar pipeline
    main(bronze_path, silver_path)