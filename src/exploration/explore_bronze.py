"""
Script para exploración inicial de datos Bronze
"""
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path


def explore_bronze_data():
    """Explora estructura y calidad de datos Bronze"""

    # Ruta relativa desde src/exploration/ -> ../../data/bronze
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"

    print("=" * 80)
    print("EXPLORACIÓN DE DATOS BRONZE")
    print("=" * 80)
    print(f"\nBuscando datos en: {bronze_path.absolute()}")

    if not bronze_path.exists():
        print(f"ERROR: No existe el directorio {bronze_path}")
        return

    # Encontrar archivos parquet
    parquet_files = list(bronze_path.rglob("*.parquet"))

    if not parquet_files:
        print("No se encontraron archivos Parquet en Bronze")
        return

    print(f"\nTotal archivos Parquet encontrados: {len(parquet_files)}")
    print(f"Primer archivo: {parquet_files[0]}")

    # Leer primer archivo para ver schema
    table = pq.read_table(str(parquet_files[0]))

    print("\n" + "=" * 80)
    print("SCHEMA DE DATOS")
    print("=" * 80)
    print(table.schema)

    print("\n" + "=" * 80)
    print("MUESTRA DE DATOS (primeras 5 filas)")
    print("=" * 80)
    df = table.to_pandas()
    print(df.head())

    print("\n" + "=" * 80)
    print("ESTADÍSTICAS BÁSICAS")
    print("=" * 80)
    print(f"Registros en este archivo: {len(table):,}")

    # Contar nulls por columna
    print("\nConteo de valores NULL por columna:")
    for col in table.column_names:
        null_count = pc.sum(pc.is_null(table[col])).as_py()
        total = len(table)
        pct = (null_count / total * 100) if total > 0 else 0
        print(f"  {col:20} : {null_count:8,} nulls ({pct:5.2f}%)")

    # Distribución de event_type
    print("\n" + "=" * 80)
    print("DISTRIBUCIÓN DE event_type")
    print("=" * 80)
    event_types = df['event_type'].value_counts()
    for event, count in event_types.items():
        pct = (count / len(df) * 100)
        print(f"  {event:10} : {count:8,} ({pct:5.2f}%)")

    # Ejemplos de category_code
    print("\n" + "=" * 80)
    print("EJEMPLOS DE category_code (primeros 10 únicos)")
    print("=" * 80)
    categories = df['category_code'].dropna().unique()[:10]
    for cat in categories:
        print(f"  {cat}")

    # Rangos de precio
    print("\n" + "=" * 80)
    print("ESTADÍSTICAS DE PRECIO")
    print("=" * 80)
    prices = df['price'].dropna()
    print(f"  Mínimo  : ${prices.min():,.2f}")
    print(f"  Máximo  : ${prices.max():,.2f}")
    print(f"  Media   : ${prices.mean():,.2f}")
    print(f"  Mediana : ${prices.median():,.2f}")


if __name__ == "__main__":
    explore_bronze_data()