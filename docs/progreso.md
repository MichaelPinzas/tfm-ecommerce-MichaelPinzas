# Progreso del Proyecto TFM

## Sesión 1 - 2026-01-16

### Completado:
- Python 3.11.9 instalado
- Estructura de carpetas creada (data, src, notebooks, docker, config, etc.)
- Repositorio Git inicializado y conectado a GitHub
- Entorno virtual Python creado y activado
- Librerías base instaladas (pytest, black, flake8, jupyter, kaggle, python-dotenv)
- Docker Compose configurado y probado
- 3 servicios Docker corriendo:
  - Zookeeper (puerto 2181)
  - Kafka (puerto 9092)
  - PostgreSQL (puerto 5432)
- PyCharm configurado con entorno virtual
- Kaggle API configurada y funcionando
- Script de descarga de dataset creado (scripts/download_kaggle_dataset.py)
- Módulos utilitarios creados:
  - src/utils/config.py (configuración centralizada)
  - src/utils/logger.py (sistema de logging)
  - src/utils/__init__.py (exports)

### Archivos creados:
- .gitignore
- README.md
- requirements.txt
- docker-compose.yml
- .env.example
- scripts/download_kaggle_dataset.py
- src/utils/config.py
- src/utils/logger.py
- src/utils/__init__.py
- docs/progreso.md

### Próxima sesión:
- Descargar dataset de Kaggle (~10GB)
- Explorar dataset con Jupyter Notebook
- Crear primer productor Kafka simple
- Crear script de carga inicial a Bronze (Parquet)
- Probar conexión a PostgreSQL desde Python

### Notas técnicas:
- Ruta proyecto: C:\Users\micha\Documents\tfm-ecommerce
- GitHub: https://github.com/MichaelPinzas/tfm-ecommerce-analytics
- Python: 3.11.9
- Docker: 27.3.1
- Kaggle API: Configurada con token
- Dataset target: mkechinov/ecommerce-behavior-data-from-multi-category-store
