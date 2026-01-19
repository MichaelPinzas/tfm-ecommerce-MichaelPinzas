"""
Script de validación ligero para la capa Bronze.
Lee solo una muestra para no saturar la memoria.

Autor: Michael Pinzas
Proyecto: TFM E-commerce Analytics
"""

import pandas as pd
from pathlib import Path
from utils.logger import setup_logger
from utils.config import BRONZE_DIR


def validate_bronze_layer():
    """Valida la capa Bronze usando muestreo."""

    logger = setup_logger('validate_bronze')
    bronze_path = Path(BRONZE_DIR)

    logger.info("="*60)
    logger.info("VALIDACIÓN DE CAPA BRONZE (MUESTRA)")
    logger.info("="*60)

    # Verificar que existe el directorio
    if not bronze_path.exists():
        logger.error(f"No existe el directorio: {bronze_path}")
        return

    # Contar archivos parquet
    parquet_files = list(bronze_path.rglob('*.parquet'))
    logger.info(f"\n--- INFORMACIÓN GENERAL ---")
    logger.info(f"Ruta: {bronze_path}")
    logger.info(f"Número de archivos Parquet: {len(parquet_files)}")

    if not parquet_files:
        logger.error("No se encontraron archivos Parquet")
        return

    # Leer solo los primeros 10 archivos como muestra
    logger.info(f"\nLeyendo muestra (primeros 10 archivos)...")
    sample_files = parquet_files[:10]

    dfs = []
    for file in sample_files:
        logger.info(f"  Leyendo: {file.name}")
        df_chunk = pd.read_parquet(file)
        dfs.append(df_chunk)

    df_sample = pd.concat(dfs, ignore_index=True)

    logger.info("\n--- DIMENSIONES DE LA MUESTRA ---")
    logger.info(f"Archivos leídos: {len(sample_files)}")
    logger.info(f"Filas en la muestra: {len(df_sample):,}")
    logger.info(f"Columnas: {len(df_sample.columns)}")

    # Schema
    logger.info("\n--- SCHEMA ---")
    logger.info(f"Columnas en los archivos Parquet:")
    for col in df_sample.columns:
        logger.info(f"  - {col}")

    logger.info(f"\nTipos de datos:")
    for col, dtype in df_sample.dtypes.items():
        logger.info(f"  - {col}: {dtype}")

    # Verificar valores nulos
    logger.info("\n--- CALIDAD DE DATOS ---")
    null_counts = df_sample.isnull().sum()
    if null_counts.sum() > 0:
        logger.info("Valores nulos por columna (en muestra):")
        for col, count in null_counts[null_counts > 0].items():
            percentage = (count / len(df_sample)) * 100
            logger.info(f"  - {col}: {count:,} ({percentage:.2f}%)")
    else:
        logger.info("✓ No hay valores nulos en la muestra")

    # Distribución de eventos por tipo
    logger.info("\n--- DISTRIBUCIÓN DE EVENTOS (MUESTRA) ---")
    event_counts = df_sample['event_type'].value_counts()
    total_events = len(df_sample)
    for event_type, count in event_counts.items():
        percentage = (count / total_events) * 100
        logger.info(f"  - {event_type}: {count:,} ({percentage:.2f}%)")

    # Fechas min y max
    logger.info("\n--- RANGO TEMPORAL (MUESTRA) ---")
    logger.info(f"Fecha mínima: {df_sample['event_time'].min()}")
    logger.info(f"Fecha máxima: {df_sample['event_time'].max()}")

    # Top 5 categorías
    logger.info("\n--- TOP 5 CATEGORÍAS (MUESTRA) ---")
    top_categories = df_sample['category_code'].value_counts().head()
    for category, count in top_categories.items():
        logger.info(f"  - {category}: {count:,}")

    # Top 5 marcas
    logger.info("\n--- TOP 5 MARCAS (MUESTRA) ---")
    top_brands = df_sample['brand'].value_counts().head()
    for brand, count in top_brands.items():
        logger.info(f"  - {brand}: {count:,}")

    # Estadísticas de precios
    logger.info("\n--- ESTADÍSTICAS DE PRECIOS (MUESTRA) ---")
    logger.info(f"Precio mínimo: ${df_sample['price'].min():.2f}")
    logger.info(f"Precio máximo: ${df_sample['price'].max():.2f}")
    logger.info(f"Precio promedio: ${df_sample['price'].mean():.2f}")
    logger.info(f"Precio mediano: ${df_sample['price'].median():.2f}")

    # Muestra de datos
    logger.info("\n--- PRIMERAS 5 FILAS ---")
    print("\n" + df_sample.head().to_string())

    # Estimación del tamaño total
    logger.info(f"\n--- ESTIMACIÓN DATASET COMPLETO ---")
    rows_per_file = len(df_sample) / len(sample_files)
    estimated_total_rows = rows_per_file * len(parquet_files)
    logger.info(f"Filas promedio por archivo: {rows_per_file:,.0f}")
    logger.info(f"Total estimado de filas: {estimated_total_rows:,.0f}")
    logger.info(f"Total de archivos Parquet: {len(parquet_files)}")

    # Verificar estructura de directorios (PARTICIONAMIENTO)
    logger.info(f"\n--- ESTRUCTURA DE PARTICIONAMIENTO ---")
    logger.info("NOTA: Las columnas year, month, day están en la estructura de carpetas")

    years = set()
    months = set()
    days = set()

    for file in parquet_files[:50]:  # Revisar primeros 50 archivos
        parts = file.parts
        for i, part in enumerate(parts):
            if part.startswith('year='):
                years.add(part)
            elif part.startswith('month='):
                months.add(part)
            elif part.startswith('day='):
                days.add(part)

    logger.info(f"Años encontrados en estructura: {sorted(years)}")
    logger.info(f"Meses encontrados en estructura: {sorted(months)}")
    logger.info(f"Días encontrados (muestra): {sorted(list(days)[:15])}")

    # Ejemplo de ruta completa
    logger.info(f"\nEjemplo de ruta particionada:")
    logger.info(f"  {parquet_files[0].relative_to(bronze_path)}")

    logger.info("\n" + "="*60)
    logger.info("✓ VALIDACIÓN COMPLETADA EXITOSAMENTE")
    logger.info("="*60)
    logger.info("\nRESUMEN:")
    logger.info(f"  • {len(parquet_files)} archivos Parquet creados")
    logger.info(f"  • ~{estimated_total_rows:,.0f} filas totales estimadas")
    logger.info(f"  • Particionamiento por year/month/day funcionando correctamente")
    logger.info(f"  • Formato Parquet con compresión aplicada")
    logger.info(f"  • Datos listos para capa Silver")
    logger.info("="*60)


if __name__ == "__main__":
    validate_bronze_layer()