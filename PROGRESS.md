# 📊 PROGRESO DEL PROYECTO TFM

**Proyecto**: Análisis del comportamiento de clientes en e-commerce mediante arquitectura Big Data  
**Autor**: Michael Pinzas Villalta  
**Repositorio**: [MichaelPinzas/tfm-ecommerce-analytics](https://github.com/MichaelPinzas/tfm-ecommerce-analytics)

---

## 🎯 OBJETIVO GENERAL

Diseñar y desplegar una arquitectura integral de Data Engineering para un e-commerce minorista de productos electrónicos, utilizando arquitectura Data Lakehouse con paradigma Medallion (Bronze-Silver-Gold).

---

## ✅ FASE 1: CONFIGURACIÓN INICIAL (COMPLETADA)

### Entorno de Desarrollo
- [x] Python 3.11.9 instalado y configurado
- [x] Entorno virtual (venv) creado
- [x] PyCharm IDE configurado
- [x] Git y GitHub configurados
- [x] Docker Compose funcionando (Kafka, Zookeeper, PostgreSQL)

### Estructura del Proyecto
```
tfm-ecommerce/
├── data/
│   ├── raw/           # Datos originales CSV
│   ├── bronze/        # Datos crudos en Parquet (particionados)
│   ├── silver/        # Datos limpios (pendiente)
│   └── gold/          # Datos agregados (pendiente)
├── src/
│   ├── ingestion/     # Scripts de ingesta
│   ├── processing/    # Transformaciones
│   ├── orchestration/ # Airflow DAGs
│   └── utils/         # Utilidades (config, logger)
├── notebooks/         # Jupyter notebooks
├── docker/            # Docker configurations
└── docs/              # Documentación
```

### Librerías Instaladas
- pandas
- pyarrow (soporte Parquet)
- kaggle (descarga de datasets)
- python-dotenv
- psycopg2-binary

---

## ✅ FASE 2: DATOS Y CAPA BRONZE (COMPLETADA)

### Dataset
**Fuente**: Kaggle - "eCommerce behavior data from Multi Category Store"  
**Tamaño total**: ~14 GB (2 archivos CSV)

| Archivo | Tamaño | Filas | Período |
|---------|--------|-------|---------|
| 2019-Oct.csv | 5.28 GB | 42,448,764 | Octubre 2019 |
| 2019-Nov.csv | 8.39 GB | 67,501,979 | Noviembre 2019 |
| **TOTAL** | **13.67 GB** | **109,950,743** | **Oct-Nov 2019** |

### Conversión a Capa Bronze
**Script**: `src/ingestion/csv_to_bronze.py`

**Características**:
- Lectura por chunks (100,000 filas) para optimizar memoria
- Conversión a formato Parquet con compresión
- Particionamiento por fecha: `year=YYYY/month=MM/day=DD`
- Logging completo con estadísticas de progreso

**Resultados**:
- ✅ 109,950,743 filas procesadas
- ✅ 1,160 archivos Parquet generados
- ✅ Tiempo de procesamiento: ~10 minutos
- ✅ Velocidad: 173,000-181,000 filas/segundo
- ✅ Estructura particionada correctamente

### Validación
**Script**: `src/validate_bronze.py`

**Schema del Dataset**:
```
- event_time: datetime64[ns, UTC]
- event_type: object (view, cart, purchase)
- product_id: int64
- category_id: int64
- category_code: object
- brand: object
- price: float64
- user_id: int64
- user_session: object
```

**Calidad de Datos**:
- 32% valores nulos en `category_code` (normal)
- 15% valores nulos en `brand` (normal)
- Sin duplicados detectados

**Distribución de Eventos**:
- 97.08% Views (navegación)
- 1.58% Purchases (compras)
- 1.34% Cart (añadir al carrito)

**Top Insights**:
- **Categorías**: Smartphones, Relojes, Notebooks, Auriculares, TVs
- **Marcas**: Samsung, Apple, Xiaomi, Huawei, Lucente
- **Precios**: $0.00 - $2,574.07 (promedio: $297.86, mediana: $160.62)

---

## 🔄 FASE 3: CAPA SILVER (PENDIENTE)

### Objetivos
- [ ] Limpieza y normalización de datos
- [ ] Deduplicación de registros
- [ ] Enriquecimiento con datos de referencia
- [ ] Validaciones de calidad avanzadas
- [ ] Creación de dimensiones (usuarios, productos, categorías)
- [ ] Cálculo de métricas derivadas

### Transformaciones Planificadas
- Estandarizar formatos de fecha/hora
- Normalizar nombres de marcas y categorías
- Calcular duración de sesiones
- Identificar rutas de conversión
- Detectar anomalías (precios negativos, sesiones sospechosas)

---

## 📅 FASE 4: CAPA GOLD (PENDIENTE)

### Objetivos
- [ ] Agregaciones por día/semana/mes
- [ ] Métricas de negocio (conversión, retención, abandono)
- [ ] Tablas optimizadas para BI
- [ ] Modelo estrella para análisis dimensional

---

## 🔧 FASE 5: ORQUESTACIÓN (PENDIENTE)

### Objetivos
- [ ] Configurar Apache Airflow
- [ ] Crear DAGs para pipelines Bronze→Silver→Gold
- [ ] Automatizar ejecución periódica
- [ ] Implementar alertas y monitoreo

---

## 📊 FASE 6: VISUALIZACIÓN (PENDIENTE)

### Objetivos
- [ ] Conectar Power BI a capa Gold
- [ ] Dashboard de ventas y conversión
- [ ] Análisis de comportamiento del usuario
- [ ] Métricas de productos y categorías

---

## 📝 COMMITS EN GIT

| # | Fecha | Descripción |
|---|-------|-------------|
| 8 | 2026-01-18 | feat: Implementar conversión CSV a Parquet para capa Bronze |
| 7 | 2026-01-16 | Initial project structure and configuration |
| ... | ... | ... |

---

## 🎯 PRÓXIMOS PASOS INMEDIATOS

1. **Documentación**: Actualizar sección "6. Solución tecnológica" del TFM
2. **Planificación**: Diseñar arquitectura de capa Silver
3. **Implementación**: Desarrollar script de transformación Silver
4. **Validación**: Crear tests para capa Silver

---

## 📚 RECURSOS Y REFERENCIAS

### Dataset
- [Kaggle: eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

### Tecnologías
- Python 3.11.9
- Apache Kafka 3.x
- PostgreSQL 15
- Apache Spark (pendiente integración)
- Delta Lake (pendiente integración)
- Apache Airflow (pendiente integración)
- Power BI (pendiente integración)

### Documentación
- [Apache Parquet Format](https://parquet.apache.org/docs/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

## 📌 NOTAS

- Trabajo con enfoque incremental ("modo tortuga" 🐢)
- Validación en cada paso antes de avanzar
- Documentación paralela al desarrollo
- Budget: <$20/mes (solución local con Docker)
- Fecha límite entrega: 5-8 Febrero 2026

---

**Última actualización**: 2026-01-18  
**Estado general**: ✅ Fase 1 y 2 completadas | 🔄 Fase 3 en planificación
