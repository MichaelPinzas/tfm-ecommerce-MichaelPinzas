"""
Script de prueba para calculate_conversion_stage()
Valida que el funnel de conversión se calcule correctamente
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from bronze_to_silver import get_spark_session, clean_data, calculate_conversion_stage
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Prueba la función calculate_conversion_stage con una muestra de datos"""
    
    # Configurar rutas
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"
    
    logger.info("="*80)
    logger.info("PRUEBA: calculate_conversion_stage()")
    logger.info("="*80)
    
    # Crear sesión Spark
    spark = get_spark_session("Test Conversion Stage")
    
    # Leer muestra de datos (200k para tener más variedad de comportamientos)
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
    
    # Tomar muestra más grande para tener variedad de comportamientos
    pdf = table.to_pandas().head(200000)
    logger.info(f"Muestra: {len(pdf):,} registros")
    
    # Convertir a Spark DataFrame
    df = spark.createDataFrame(pdf)
    
    # Aplicar clean_data primero
    df = clean_data(df)
    
    # Aplicar calculate_conversion_stage
    logger.info("\n" + "="*80)
    logger.info("EJECUTANDO: calculate_conversion_stage()")
    logger.info("="*80)
    df_staged = calculate_conversion_stage(df)
    
    # Validaciones
    logger.info("\n" + "="*80)
    logger.info("VALIDACIONES")
    logger.info("="*80)
    
    # 1. Verificar que se agregó la columna
    logger.info("\n1. Columna agregada:")
    if "conversion_stage" in df_staged.columns:
        logger.info("   ✅ conversion_stage")
    else:
        logger.error("   ❌ conversion_stage NO ENCONTRADA")
    
    # 2. Verificar que user_product_events fue eliminada
    if "user_product_events" in df_staged.columns:
        logger.warning("   ⚠️ user_product_events NO fue eliminada")
    else:
        logger.info("   ✅ user_product_events eliminada correctamente")
    
    # 3. Distribución del funnel de conversión
    logger.info("\n2. Distribución del funnel de conversión:")
    funnel_dist = df_staged.groupBy("conversion_stage").count() \
        .orderBy("count", ascending=False) \
        .collect()
    
    total = df_staged.count()
    for row in funnel_dist:
        pct = (row['count'] / total) * 100
        logger.info(f"   {row['conversion_stage']:>15s}: {row['count']:>7,} ({pct:>5.2f}%)")
    
    # 4. Distribución por tipo de evento vs conversion_stage
    logger.info("\n3. Relación event_type vs conversion_stage:")
    event_stage_dist = df_staged.groupBy("event_type", "conversion_stage").count() \
        .orderBy("event_type", "conversion_stage") \
        .collect()
    
    for row in event_stage_dist:
        logger.info(f"   {row['event_type']:>10s} + {row['conversion_stage']:>15s}: {row['count']:>7,}")
    
    # 5. Análisis de usuarios únicos por etapa
    logger.info("\n4. Usuarios únicos por etapa del funnel:")
    
    unique_users_viewed = df_staged.filter(
        df_staged.conversion_stage == "viewed_only"
    ).select("user_id").distinct().count()
    
    unique_users_cart = df_staged.filter(
        df_staged.conversion_stage == "added_to_cart"
    ).select("user_id").distinct().count()
    
    unique_users_purchased = df_staged.filter(
        df_staged.conversion_stage == "purchased"
    ).select("user_id").distinct().count()
    
    logger.info(f"   Usuarios que solo vieron: {unique_users_viewed:,}")
    logger.info(f"   Usuarios que agregaron al carrito: {unique_users_cart:,}")
    logger.info(f"   Usuarios que compraron: {unique_users_purchased:,}")
    
    # 6. Ejemplos de cada etapa
    logger.info("\n5. Ejemplos de cada etapa del funnel:")
    
    logger.info("\n   a) viewed_only (usuarios que solo vieron):")
    df_staged.filter(df_staged.conversion_stage == "viewed_only") \
        .select("user_id", "product_id", "event_type", "conversion_stage") \
        .distinct() \
        .show(5, truncate=False)
    
    logger.info("   b) added_to_cart (agregaron al carrito pero no compraron):")
    df_staged.filter(df_staged.conversion_stage == "added_to_cart") \
        .select("user_id", "product_id", "event_type", "conversion_stage") \
        .distinct() \
        .show(5, truncate=False)
    
    logger.info("   c) purchased (compraron el producto):")
    df_staged.filter(df_staged.conversion_stage == "purchased") \
        .select("user_id", "product_id", "event_type", "conversion_stage") \
        .distinct() \
        .show(5, truncate=False)
    
    # 7. Validación de lógica: un usuario que compró debe tener todos sus eventos marcados como "purchased"
    logger.info("\n6. Validación de lógica del funnel:")
    
    # Buscar un usuario que haya comprado
    purchase_user = df_staged.filter(df_staged.event_type == "purchase") \
        .select("user_id", "product_id") \
        .limit(1) \
        .collect()
    
    if purchase_user:
        user_id = purchase_user[0]['user_id']
        product_id = purchase_user[0]['product_id']
        
        logger.info(f"\n   Usuario de ejemplo: {user_id}, Producto: {product_id}")
        logger.info("   Todos sus eventos para este producto:")
        
        df_staged.filter(
            (df_staged.user_id == user_id) & (df_staged.product_id == product_id)
        ).select("event_time", "event_type", "conversion_stage") \
         .orderBy("event_time") \
         .show(truncate=False)
    
    logger.info("\n" + "="*80)
    logger.info("✅ PRUEBA COMPLETADA")
    logger.info("="*80)
    
    spark.stop()

if __name__ == "__main__":
    main()
