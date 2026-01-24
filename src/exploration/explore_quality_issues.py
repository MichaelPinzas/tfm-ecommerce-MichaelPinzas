"""
Script para explorar problemas de calidad específicos en Bronze
- Duplicados
- Precios = 0
- Distribución category_l1
"""
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path
import pandas as pd


def explore_quality_issues():
    """Explora problemas de calidad en una muestra de Bronze"""

    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / "data" / "bronze"

    print("=" * 80)
    print("ANÁLISIS DE CALIDAD DE DATOS BRONZE")
    print("=" * 80)

    # Leer varios archivos para muestra representativa
    parquet_files = list(bronze_path.rglob("*.parquet"))

    # Tomar muestra: primeros 10 archivos
    sample_files = parquet_files[:10]
    print(f"\nAnalizando muestra de {len(sample_files)} archivos (de {len(parquet_files)} totales)")

    # Leer y concatenar
    tables = []
    for f in sample_files:
        tables.append(pq.read_table(str(f)))

    # Concatenar en un solo DataFrame pandas para análisis
    df = pd.concat([t.to_pandas() for t in tables], ignore_index=True)

    print(f"Registros en muestra: {len(df):,}")

    # ===== 1. ANÁLISIS DE DUPLICADOS =====
    print("\n" + "=" * 80)
    print("1. ANÁLISIS DE DUPLICADOS")
    print("=" * 80)

    # Duplicados por combinación user_id + product_id + event_type + timestamp
    print("\nCriterio: user_id + product_id + event_type + timestamp")

    # Redondear timestamp a segundo para identificar duplicados
    df['timestamp_second'] = df['event_time'].dt.floor('S')

    duplicates = df.duplicated(
        subset=['user_id', 'product_id', 'event_type', 'timestamp_second'],
        keep=False
    )

    num_duplicates = duplicates.sum()
    pct_duplicates = (num_duplicates / len(df) * 100)

    print(f"  Total duplicados: {num_duplicates:,} ({pct_duplicates:.4f}%)")

    if num_duplicates > 0:
        print("\nEjemplo de duplicados:")
        dup_sample = df[duplicates].head(10)[
            ['event_time', 'event_type', 'product_id', 'user_id', 'price']
        ]
        print(dup_sample)

    # ===== 2. ANÁLISIS DE PRECIOS = 0 =====
    print("\n" + "=" * 80)
    print("2. ANÁLISIS DE PRECIOS INVÁLIDOS")
    print("=" * 80)

    zero_prices = (df['price'] == 0).sum()
    negative_prices = (df['price'] < 0).sum()
    valid_prices = (df['price'] > 0).sum()

    pct_zero = (zero_prices / len(df) * 100)
    pct_negative = (negative_prices / len(df) * 100)
    pct_valid = (valid_prices / len(df) * 100)

    print(f"  Precios = 0       : {zero_prices:,} ({pct_zero:.4f}%)")
    print(f"  Precios < 0       : {negative_prices:,} ({pct_negative:.4f}%)")
    print(f"  Precios válidos   : {valid_prices:,} ({pct_valid:.2f}%)")

    if zero_prices > 0:
        print("\nEjemplos de precios = 0:")
        zero_sample = df[df['price'] == 0].head(5)[
            ['event_time', 'event_type', 'product_id', 'brand', 'price']
        ]
        print(zero_sample)

    # ===== 3. DISTRIBUCIÓN CATEGORY_L1 =====
    print("\n" + "=" * 80)
    print("3. DISTRIBUCIÓN DE CATEGORÍAS (nivel 1)")
    print("=" * 80)

    # Parsear category_code
    def parse_category_l1(cat_code):
        if pd.isna(cat_code) or cat_code == "Unknown":
            return "Unknown"
        parts = str(cat_code).split('.')
        return parts[0] if parts else "Unknown"

    df['category_l1_parsed'] = df['category_code'].apply(parse_category_l1)

    # Top 15 categorías nivel 1
    cat_l1_dist = df['category_l1_parsed'].value_counts().head(15)

    print("\nTop 15 categorías nivel 1:")
    for cat, count in cat_l1_dist.items():
        pct = (count / len(df) * 100)
        print(f"  {cat:25} : {count:8,} ({pct:5.2f}%)")

    # Verificar si hay categorías raras
    print(f"\nTotal categorías únicas nivel 1: {df['category_l1_parsed'].nunique()}")

    # Mostrar algunas categorías únicas (las menos comunes)
    rare_cats = df['category_l1_parsed'].value_counts().tail(10)
    if len(rare_cats) > 0:
        print("\nCategorías menos comunes (últimas 10):")
        for cat, count in rare_cats.items():
            print(f"  {cat:25} : {count:8,}")

    # ===== RESUMEN Y RECOMENDACIONES =====
    print("\n" + "=" * 80)
    print("RESUMEN Y RECOMENDACIONES")
    print("=" * 80)

    print("\n1. DUPLICADOS:")
    if pct_duplicates < 0.1:
        print("   ✅ Muy pocos duplicados (<0.1%). Filtro simple suficiente.")
    elif pct_duplicates < 2:
        print("   ⚠️  Duplicados moderados (<2%). Implementar deduplicación.")
    else:
        print("   🚨 Duplicados significativos (>2%). Investigar causa.")

    print("\n2. PRECIOS INVÁLIDOS:")
    if pct_zero + pct_negative < 0.1:
        print("   ✅ Muy pocos precios inválidos. Filtro simple suficiente.")
    else:
        print(f"   ⚠️  {pct_zero + pct_negative:.2f}% precios inválidos. Filtrar en Silver.")

    print("\n3. CATEGORÍAS:")
    num_unique_l1 = df['category_l1_parsed'].nunique()
    if num_unique_l1 < 20:
        print(f"   ✅ Categorías limpias ({num_unique_l1} únicas nivel 1).")
    else:
        print(f"   ⚠️  Muchas categorías ({num_unique_l1} únicas). Revisar consistencia.")


if __name__ == "__main__":
    explore_quality_issues()