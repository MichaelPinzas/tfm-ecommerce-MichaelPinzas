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

    # Extraer hora del día
    df = df.withColumn("hour", hour(col("event_time")))

    # Extraer día de la semana (1=Monday, 7=Sunday)
    df = df.withColumn("day_of_week", dayofweek(col("event_time")))

    # Identificar si es fin de semana (Saturday=7, Sunday=1)
    df = df.withColumn(
        "is_weekend",
        when((col("day_of_week") == 1) | (col("day_of_week") == 7), True)
        .otherwise(False)
    )

    # Clasificar por momento del día
    df = df.withColumn(
        "time_of_day",
        when((col("hour") >= 6) & (col("hour") < 12), "morning")
        .when((col("hour") >= 12) & (col("hour") < 18), "afternoon")
        .when((col("hour") >= 18) & (col("hour") < 22), "evening")
        .otherwise("night")
    )

    logger.info("✅ Columnas temporales agregadas: hour, day_of_week, is_weekend, time_of_day")

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

    # Dividir category_code por "."
    # split() devuelve un array: ["electronics", "smartphone", "apple"]
    df = df.withColumn("category_split", split(col("category_code"), "\\."))

    # Extraer nivel 1 (siempre existe, al menos "Unknown")
    df = df.withColumn(
        "category_l1",
        col("category_split").getItem(0)
    )

    # Extraer nivel 2 (puede ser null si solo hay 1 nivel)
    df = df.withColumn(
        "category_l2",
        when(size(col("category_split")) > 1, col("category_split").getItem(1))
        .otherwise(lit(None))
    )

    # Extraer nivel 3 (puede ser null si solo hay 1-2 niveles)
    df = df.withColumn(
        "category_l3",
        when(size(col("category_split")) > 2, col("category_split").getItem(2))
        .otherwise(lit(None))
    )

    # Eliminar columna temporal
    df = df.drop("category_split")

    logger.info("✅ Categorías parseadas: category_l1, category_l2, category_l3")

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

    # Importar funciones necesarias
    from pyspark.sql.functions import collect_set, array_contains

    # Crear ventana por user_id + product_id para ver todos los eventos
    window_spec = Window.partitionBy("user_id", "product_id")

    # Recolectar todos los event_types para cada combinación user+product
    df = df.withColumn(
        "user_product_events",
        collect_set(col("event_type")).over(window_spec)
    )

    # Lógica de clasificación del funnel:
    # 1. Si tiene 'purchase' en sus eventos → purchased
    # 2. Si tiene 'cart' pero NO 'purchase' → added_to_cart
    # 3. Si solo tiene 'view' → viewed_only
    df = df.withColumn(
        "conversion_stage",
        when(
            array_contains(col("user_product_events"), "purchase"),
            lit("purchased")
        )
        .when(
            array_contains(col("user_product_events"), "cart"),
            lit("added_to_cart")
        )
        .otherwise(lit("viewed_only"))
    )

    # Eliminar columna temporal
    df = df.drop("user_product_events")

    logger.info("✅ Conversion stage calculado: viewed_only, added_to_cart, purchased")

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

    # MODIFICACIÓN: Procesar solo PRIMEROS 7 DÍAS de OCTUBRE
    # Esto procesa ~3-4M registros (manejable en memoria)
    logger.info("⚠️  PROCESANDO SOLO PRIMEROS 7 DÍAS DE OCTUBRE 2019")
    logger.info("   (days 01-07, ~3-4M registros)")
    
    # Leer solo los primeros 7 días de octubre
    days_to_process = [f"day={i}" for i in range(1, 8)]  # day=1 hasta day=7
    bronze_week_paths = [
        bronze_path / "year=2019" / "month=10" / day 
        for day in days_to_process
    ]
    
    all_data = []
    for day_path in bronze_week_paths:
        if day_path.exists():
            logger.info(f"Leyendo: {day_path.name}")
            table = pq.read_table(str(day_path))
            
            # Convertir timestamp de nanosegundos a microsegundos
            table = table.cast(table.schema.set(
                table.schema.get_field_index('event_time'),
                pa.field('event_time', pa.timestamp('us', tz='UTC'))
            ))
            
            all_data.append(table)
    
    # Concatenar todas las tablas
    logger.info("Concatenando datos de los 7 días...")
    combined_table = pa.concat_tables(all_data)
    
    # Convertir a pandas
    pdf = combined_table.to_pandas()
    logger.info(f"Registros leídos: {len(pdf):,}")

    # Convertir pandas a Spark DataFrame
    df = spark.createDataFrame(pdf)

    # Aplicar transformaciones secuencialmente
    df = clean_data(df)
    df = enrich_temporal(df)
    df = parse_categories(df)
    df = calculate_conversion_stage(df)

    # Agregar columnas de particionamiento (year, month, day)
    # Las extraemos de event_time para poder particionar
    from pyspark.sql.functions import year, month, dayofmonth
    logger.info("Agregando columnas de particionamiento (year, month, day)...")
    df = df.withColumn("year", year(col("event_time"))) \
           .withColumn("month", month(col("event_time"))) \
           .withColumn("day", dayofmonth(col("event_time")))

    # Escribir a Silver manteniendo particionamiento
    logger.info(f"Escribiendo datos a: {silver_path}")
    df.write \
        .partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .parquet(str(silver_path))

    final_count = df.count()
    logger.info(f"Registros finales en Silver: {final_count:,}")
    logger.info("✅ Transformación completada exitosamente")
    logger.info("ℹ️  Procesados primeros 7 días de Octubre 2019")

    spark.stop()


if __name__ == "__main__":
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"
    silver_path = project_root / "data" / "silver" / "events"

    # Ejecutar pipeline
    main(bronze_path, silver_path)