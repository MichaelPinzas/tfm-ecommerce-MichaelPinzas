"""
Script para convertir archivos CSV del dataset de e-commerce a formato Parquet
y almacenarlos en la capa Bronze con particionamiento por fecha.

"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import logging

# Importar utilidades
from utils.logger import setup_logger
from utils.config import RAW_DATA_DIR, BRONZE_DIR, ensure_directories

class CSVToBronzeConverter:
    """
    Convierte archivos CSV a Parquet con particionamiento por fecha
    para la capa Bronze del Data Lakehouse.
    """

    def __init__(self):
        """Inicializa el convertidor."""
        self.logger = setup_logger('csv_to_bronze')
        self.bronze_path = Path(BRONZE_DIR)
        self.raw_path = Path(RAW_DATA_DIR)

        # Asegurar que existen los directorios
        ensure_directories()

        self.logger.info(f"Ruta raw: {self.raw_path}")
        self.logger.info(f"Ruta bronze: {self.bronze_path}")

    def read_csv_chunks(self, csv_file: Path, chunksize: int = 100000):
        """
        Lee un archivo CSV en chunks para evitar problemas de memoria.

        Args:
            csv_file: Ruta al archivo CSV
            chunksize: Número de filas por chunk

        Yields:
            DataFrame con un chunk de datos
        """
        self.logger.info(f"Leyendo archivo: {csv_file.name}")

        try:
            for chunk_num, chunk in enumerate(pd.read_csv(
                csv_file,
                chunksize=chunksize,
                parse_dates=['event_time']
            ), 1):
                self.logger.debug(f"Procesando chunk {chunk_num} ({len(chunk)} filas)")
                yield chunk

        except Exception as e:
            self.logger.error(f"Error leyendo CSV: {str(e)}")
            raise

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara el DataFrame añadiendo columnas de particionamiento.

        Args:
            df: DataFrame original

        Returns:
            DataFrame con columnas de particionamiento (year, month, day)
        """
        # Extraer componentes de fecha para particionamiento
        df['year'] = df['event_time'].dt.year
        df['month'] = df['event_time'].dt.month
        df['day'] = df['event_time'].dt.day

        return df

    def write_parquet_partitioned(
        self,
        df: pd.DataFrame,
        base_path: Path,
        partition_cols: list = ['year', 'month', 'day']
    ):
        """
        Escribe DataFrame en formato Parquet con particionamiento.

        Args:
            df: DataFrame a escribir
            base_path: Ruta base donde guardar los archivos
            partition_cols: Columnas para particionar
        """
        try:
            # Convertir a tabla PyArrow
            table = pa.Table.from_pandas(df)

            # Escribir con particionamiento
            pq.write_to_dataset(
                table,
                root_path=str(base_path),
                partition_cols=partition_cols,
                existing_data_behavior='overwrite_or_ignore'
            )

        except Exception as e:
            self.logger.error(f"Error escribiendo Parquet: {str(e)}")
            raise

    def convert_file(self, csv_filename: str, chunksize: int = 100000):
        """
        Convierte un archivo CSV completo a Parquet particionado.

        Args:
            csv_filename: Nombre del archivo CSV en la carpeta raw
            chunksize: Tamaño de chunks para procesar
        """
        csv_path = self.raw_path / csv_filename

        if not csv_path.exists():
            self.logger.error(f"Archivo no encontrado: {csv_path}")
            raise FileNotFoundError(f"No existe el archivo: {csv_path}")

        self.logger.info(f"Iniciando conversión de {csv_filename}")
        self.logger.info(f"Tamaño del archivo: {csv_path.stat().st_size / (1024**3):.2f} GB")

        start_time = datetime.now()
        total_rows = 0

        try:
            # Procesar archivo en chunks
            for chunk in self.read_csv_chunks(csv_path, chunksize):
                # Preparar datos
                chunk = self.prepare_dataframe(chunk)

                # Escribir a Bronze
                self.write_parquet_partitioned(chunk, self.bronze_path)

                total_rows += len(chunk)
                self.logger.info(f"Procesadas {total_rows:,} filas hasta ahora...")

            # Estadísticas finales
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.info("=" * 60)
            self.logger.info("CONVERSIÓN COMPLETADA")
            self.logger.info(f"Archivo: {csv_filename}")
            self.logger.info(f"Total de filas procesadas: {total_rows:,}")
            self.logger.info(f"Tiempo total: {duration:.2f} segundos")
            self.logger.info(f"Velocidad: {total_rows/duration:.0f} filas/segundo")
            self.logger.info(f"Datos guardados en: {self.bronze_path}")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Error durante la conversión: {str(e)}")
            raise

    def convert_all_files(self, pattern: str = "*.csv", chunksize: int = 100000):
        """
        Convierte todos los archivos CSV que coincidan con el patrón.

        Args:
            pattern: Patrón de archivos a procesar
            chunksize: Tamaño de chunks para procesar
        """
        csv_files = list(self.raw_path.glob(pattern))

        if not csv_files:
            self.logger.warning(f"No se encontraron archivos CSV en {self.raw_path}")
            return

        self.logger.info(f"Archivos encontrados: {len(csv_files)}")
        for csv_file in csv_files:
            self.logger.info(f"  - {csv_file.name}")

        for csv_file in csv_files:
            self.convert_file(csv_file.name, chunksize)


def main():
    """Función principal para ejecutar la conversión."""

    # Configurar logging básico
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Crear convertidor
    converter = CSVToBronzeConverter()

    # Convertir todos los archivos CSV
    converter.convert_all_files(chunksize=100000)


if __name__ == "__main__":
    main()