# Progreso del Proyecto TFM

## Sesión 1 - 2026-01-16

### COMPLETADO:

**Infraestructura:**
- Python 3.11.9 instalado y configurado
- Estructura completa de carpetas del proyecto
- Repositorio Git inicializado
- GitHub conectado: https://github.com/MichaelPinzas/tfm-ecommerce-analytics
- Entorno virtual Python activo
- Docker Compose configurado y probado

**Servicios Docker funcionando:**
- Zookeeper (puerto 2181)
- Kafka (puerto 9092)
- PostgreSQL (puerto 5432)

**Herramientas configuradas:**
- PyCharm con entorno virtual
- Kaggle API configurada
- Jupyter Notebook funcionando

**Librerías instaladas:**
- Testing: pytest, pytest-cov
- Code quality: black, flake8
- Data: pandas, numpy
- Notebooks: jupyter, notebook, ipykernel
- Utilities: kaggle, python-dotenv

**Código desarrollado:**
- scripts/download_kaggle_dataset.py - Script de descarga de Kaggle
- src/utils/config.py - Configuración centralizada
- src/utils/logger.py - Sistema de logging
- src/utils/__init__.py - Exports del módulo
- notebooks/01_exploracion_inicial_dataset.ipynb - Análisis exploratorio

**Dataset descargado:**
- Fuente: Kaggle - eCommerce behavior data from Multi Category Store
- Archivos: 2019-Oct.csv (5.4 GB) + 2019-Nov.csv (8.6 GB)
- Total: ~14 GB de datos reales
- Registros analizados: 100,000 (muestra inicial)

**Insights del dataset:**
- Columnas: event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
- Distribución eventos: 97.1% views, 1.7% purchases, 1.2% cart
- Valores nulos: category_code (~25%), brand (~35%)

**Commits realizados:** 6

### PRÓXIMA SESIÓN:

**Prioridad Alta:**
1. Crear script de conversión CSV → Parquet (capa Bronze)
2. Implementar particionamiento por fecha
3. Análisis completo del dataset en notebook
4. Crear primer productor Kafka de prueba

**Prioridad Media:**
5. Conectar Python con PostgreSQL
6. Crear esquema de base de datos transaccional
7. Script de limpieza para capa Silver

**Planificación:**
- Siguiente fase: Ingesta y transformación a Bronze
- Tiempo estimado: 2-3 sesiones más
- Objetivo: Pipeline Bronze funcionando con datos reales

### Notas técnicas:
- Ruta: C:\Users\micha\Documents\tfm-ecommerce
- Python: 3.11.9
- Docker: 27.3.1
- Dataset: 109M registros (~14GB)
