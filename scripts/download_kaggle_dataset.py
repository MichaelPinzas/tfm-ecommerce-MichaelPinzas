"""
Script para descargar el dataset de e-commerce de Kaggle.
Dataset: eCommerce behavior data from Multi Category Store
URL: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
"""

import os
import sys
from pathlib import Path

# Añadir el directorio raíz al path para imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def download_ecommerce_dataset():
    """
    Descarga el dataset de e-commerce de Kaggle.

    El dataset contiene ~109M registros de comportamiento de usuarios
    en un e-commerce de productos electrónicos.
    """
    import kaggle

    # Dataset identificador en Kaggle
    dataset_name = "mkechinov/ecommerce-behavior-data-from-multi-category-store"

    # Directorio de descarga
    download_path = project_root / "data" / "raw"
    download_path.mkdir(parents=True, exist_ok=True)

    print(f" Descargando dataset: {dataset_name}")
    print(f" Directorio destino: {download_path}")
    print(" Este proceso puede tardar varios minutos (dataset ~10GB)...")

    try:
        # Descargar dataset
        kaggle.api.dataset_download_files(
            dataset_name,
            path=str(download_path),
            unzip=True
        )
        print(" Dataset descargado exitosamente!")
        print(f" Archivos en: {download_path}")

        # Listar archivos descargados
        files = list(download_path.glob("*.csv"))
        print(f"\n📄 Archivos CSV encontrados: {len(files)}")
        for file in files:
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"   - {file.name} ({size_mb:.2f} MB)")

    except Exception as e:
        print(f" Error al descargar dataset: {e}")
        sys.exit(1)


if __name__ == "__main__":
    download_ecommerce_dataset()