"""
Script de prueba para parse_categories()
Valida que las categorías se parseen correctamente en niveles jerárquicos
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from bronze_to_silver import get_spark_session, clean_data, parse_categories
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Prueba la función parse_categories con una muestra de datos"""
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"
    
    logger.info("="*80)
    logger.info("PRUEBA: parse_categories()")
    logger.info("="*80)
    
    # Crear sesión Spark
    spark = get_spark_session("Test Parse Categories")
    
    # Leer muestra de datos (100k registros para tener más variedad)
    logger.info(f"Leyendo muestra desde: {bronze_path}")
    table = pq.read_table(
        str(bronze_path),
        columns=['event_time', 'event_type', 'product_id', 'user_id', 
                'category_code', 'brand', 'price']
    )
    
    # Convertir timestamp a microsegundos
    table = table.cast(table.schema.set(
        table.schema.get_field_index('event_time'),
        pa.field('event_time', pa.timestamp('us', tz='UTC'))
    ))
    
    # Tomar muestra más grande para tener variedad de categorías
    pdf = table.to_pandas().head(100000)
    logger.info(f"Muestra: {len(pdf):,} registros")
    
    # Convertir a Spark DataFrame
    df = spark.createDataFrame(pdf)
    
    # Aplicar clean_data primero
    df = clean_data(df)
    
    # Aplicar parse_categories
    logger.info("\n" + "="*80)
    logger.info("EJECUTANDO: parse_categories()")
    logger.info("="*80)
    df_parsed = parse_categories(df)
    
    # Validaciones
    logger.info("\n" + "="*80)
    logger.info("VALIDACIONES")
    logger.info("="*80)
    
    # 1. Verificar que se agregaron las columnas
    expected_cols = ['category_l1', 'category_l2', 'category_l3']
    actual_cols = df_parsed.columns
    
    logger.info("\n1. Columnas agregadas:")
    for col in expected_cols:
        if col in actual_cols:
            logger.info(f"   ✅ {col}")
        else:
            logger.error(f"   ❌ {col} NO ENCONTRADA")
    
    # 2. Verificar que category_split fue eliminada
    if "category_split" in df_parsed.columns:
        logger.warning("   ⚠️ category_split NO fue eliminada")
    else:
        logger.info("   ✅ category_split eliminada correctamente")
    
    # 3. Distribución de niveles de categoría
    logger.info("\n2. Categorías únicas por nivel:")
    
    l1_count = df_parsed.select("category_l1").distinct().count()
    logger.info(f"   category_l1: {l1_count} categorías únicas")
    
    l2_count = df_parsed.select("category_l2").distinct().count()
    l2_nulls = df_parsed.filter(df_parsed.category_l2.isNull()).count()
    logger.info(f"   category_l2: {l2_count} categorías únicas ({l2_nulls:,} nulls)")
    
    l3_count = df_parsed.select("category_l3").distinct().count()
    l3_nulls = df_parsed.filter(df_parsed.category_l3.isNull()).count()
    logger.info(f"   category_l3: {l3_count} categorías únicas ({l3_nulls:,} nulls)")
    
    # 4. Top 10 categorías nivel 1
    logger.info("\n3. Top 10 categorías nivel 1:")
    top_l1 = df_parsed.groupBy("category_l1").count() \
        .orderBy("count", ascending=False) \
        .limit(10) \
        .collect()
    
    for row in top_l1:
        pct = (row['count'] / df_parsed.count()) * 100
        logger.info(f"   {row['category_l1']:>15s}: {row['count']:>6,} ({pct:>5.2f}%)")
    
    # 5. Ejemplos de parsing
    logger.info("\n4. Ejemplos de parsing de categorías:")
    df_parsed.select(
        "category_code", "category_l1", "category_l2", "category_l3"
    ).distinct().show(15, truncate=False)
    
    # 6. Casos especiales: Unknown y categorías sin sub-niveles
    logger.info("\n5. Casos especiales:")
    
    unknown_count = df_parsed.filter(df_parsed.category_l1 == "Unknown").count()
    logger.info(f"   Registros con 'Unknown': {unknown_count:,}")
    
    single_level = df_parsed.filter(
        (df_parsed.category_l2.isNull()) & (df_parsed.category_l1 != "Unknown")
    ).count()
    logger.info(f"   Categorías de 1 solo nivel: {single_level:,}")
    
    two_level = df_parsed.filter(
        df_parsed.category_l2.isNotNull() & df_parsed.category_l3.isNull()
    ).count()
    logger.info(f"   Categorías de 2 niveles: {two_level:,}")
    
    three_level = df_parsed.filter(df_parsed.category_l3.isNotNull()).count()
    logger.info(f"   Categorías de 3 niveles: {three_level:,}")
    
    logger.info("\n" + "="*80)
    logger.info("✅ PRUEBA COMPLETADA")
    logger.info("="*80)
    
    spark.stop()

if __name__ == "__main__":
    main()
