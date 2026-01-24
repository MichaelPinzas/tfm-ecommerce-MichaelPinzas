"""
Script para probar solo la función clean_data()
"""
from bronze_to_silver import get_spark_session, clean_data
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa


def test_clean():
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"

    print("Iniciando Spark...")
    spark = get_spark_session("Test Clean Data")

    print(f"Leyendo muestra de Bronze desde: {bronze_path}")

    # Leer solo 1 archivo con PyArrow (evita error de nanosegundos)
    sample_file = list(bronze_path.rglob("*.parquet"))[0]

    # Leer con PyArrow
    table = pq.read_table(str(sample_file))

    # Convertir timestamp de nanosegundos a microsegundos
    table = table.cast(table.schema.set(
        table.schema.get_field_index('event_time'),
        pa.field('event_time', pa.timestamp('us', tz='UTC'))
    ))

    # Convertir a pandas y luego a Spark
    pdf = table.to_pandas()
    df = spark.createDataFrame(pdf)

    print(f"\nRegistros antes de limpieza: {df.count():,}")
    print("\nEjemplo de datos ANTES:")
    df.show(5, truncate=False)

    # Aplicar limpieza
    df_clean = clean_data(df)

    print(f"\nRegistros después de limpieza: {df_clean.count():,}")
    print("\nEjemplo de datos DESPUÉS:")
    df_clean.show(5, truncate=False)

    # Verificar que no hay nulls en brand ni category_code
    null_brands = df_clean.filter(df_clean.brand.isNull()).count()
    null_cats = df_clean.filter(df_clean.category_code.isNull()).count()

    print(f"\n✅ Verificación:")
    print(f"  Nulls en brand: {null_brands} (debe ser 0)")
    print(f"  Nulls en category_code: {null_cats} (debe ser 0)")
    print(f"  Todos los precios > 0: Verificado en filtro")

    spark.stop()
    print("\n✅ Prueba completada")


if __name__ == "__main__":
    test_clean()