# 🏠 Real Estate Argentina Analytics

Pipeline de datos end-to-end para análisis de publicaciones inmobiliarias en Argentina.  
Stack moderno, 100% open source y gratuito.

---

## Arquitectura

```
┌─────────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────────────┐
│  Kaggle API │───▶│  DuckDB  │───▶│  dbt Core    │───▶│  Tableau Public  │
│  (fuente)   │    │  (RAW)   │    │  (transform) │    │  (dashboard)     │
└─────────────┘    └──────────┘    └──────────────┘    └──────────────────┘
                                          ▲
                                   ┌──────┴──────┐
                                   │   Prefect   │
                                   │ (orquesta)  │
                                   └─────────────┘
```

| Capa | Herramienta | Rol |
|---|---|---|
| Ingesta | `kagglehub` + `DuckDB` | Descarga y carga el CSV crudo |
| Transformación | `dbt Core` | Limpieza, tipado, métricas y modelos analíticos |
| Orquestación | `Prefect 3` | Coordina el pipeline, maneja errores y reintentos |
| Warehouse | `DuckDB` | Motor columnar analítico (equivalente local a BigQuery) |
| Visualización | `Tableau` | Dashboards conectados directamente a los marts |

---

## Estructura del proyecto

```
real_estate_argentina_analytics/
├── ingestion/
│   └── load_raw.py              # Descarga Kaggle → carga raw.properties en DuckDB
│
├── dbt_realestate/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_properties.sql   # Limpieza y tipado del CSV crudo
│   │   │   └── schema.yml           # Tests de calidad (not_null, accepted_values)
│   │   └── marts/
│   │       ├── fact_publicaciones.sql  # Tabla de hechos con todas las métricas
│   │       ├── mart_kpis_geo.sql       # KPIs por provincia/ciudad para Tableau
│   │       ├── mart_kpis_tiempo.sql    # Serie temporal mensual para Tableau
│   │       └── schema.yml
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── prefect/
│   └── pipeline.py              # Orquestación completa del pipeline
│
├── setup/
│   ├── requirements.txt
│   └── .env.example
│
└── README.md
```

---

## Modelo de datos (Star Schema)

```
                 stg_properties (VIEW)
                        │
                        ▼
              fact_publicaciones (TABLE)
              ┌─────────────────────────┐
              │ id_publicacion          │
              │ operation_type          │──▶ mart_kpis_geo
              │ currency                │         (TABLE)
              │ property_type / rooms   │
              │ pais / provincia / ciudad│──▶ mart_kpis_tiempo
              │ fecha_alta / fecha_baja  │         (TABLE)
              │ anio / mes / trimestre   │
              │ precio                  │
              │ precio_por_m2           │
              │ ingreso_comision        │
              │ dias_publicacion        │
              └─────────────────────────┘
```

**Correcciones al modelo original (TP 2021):**
- `dias_publicacion` calculado como medida en fact (no par de fechas sin métrica)
- `porcentaje_comision` en fact (era medida en dimensión)
- `dim_agente` preparada para integrar datos de CRM

---

## Cómo ejecutar

### 1. Clonar e instalar dependencias

```bash
git clone https://github.com/tu-usuario/real_estate_argentina_analytics
cd real_estate_argentina_analytics
pip install -r setup/requirements.txt
```

### 2. Configurar credenciales de Kaggle

```bash
# Descargar kaggle.json desde: kaggle.com → Settings → API → Create New Token
mkdir -p ~/.config/kaggle
mv ~/Downloads/kaggle.json ~/.config/kaggle/
chmod 600 ~/.config/kaggle/kaggle.json

# Configurar variables de entorno
cp setup/.env.example .env
# Editar .env con tu editor
```

### 3. Ejecutar el pipeline completo

```bash
# Opción A: Pipeline orquestado con Prefect (recomendado)
python prefect/pipeline.py

# Opción B: Paso a paso
python ingestion/load_raw.py       # 1. Descargar y cargar raw
cd dbt_realestate && dbt run            # 2. Transformar con dbt
cd dbt_realestate && dbt test           # 3. Validar calidad
```

### 4. Ver en Prefect Cloud (gratis)

```bash
prefect cloud login    # requiere cuenta en prefect.io (gratis)
python prefect/pipeline.py
# → el run aparece en https://app.prefect.cloud con logs y métricas
```

### 5. Conectar Tableau

Los marts se exportan automáticamente como CSV en `data/tableau/`.  
En Tableau Desktop/Public: **Connect → Text File → fact_publicaciones.csv**

---

## Dataset

- **Fuente:** [Kaggle — Argentina Venta de Propiedades](https://www.kaggle.com/datasets/msorondo/argentina-venta-de-propiedades)
- **Volumen:** ~2M filas de publicaciones históricas (2019-2021)
- **Columnas clave:** `operation_type`, `currency`, `property_type`, `rooms`, `l1/l2/l3`, `start_date`, `end_date`, `surface_covered`, `surface_total`, `price`
- **Limitación conocida:** datos sin actualización desde 2021, sin información de agentes

---

## KPIs implementados

| KPI | Modelo dbt | Responde a |
|---|---|---|
| Operaciones por zona | `mart_kpis_geo` | Gerencia de Ventas |
| Tiempo promedio de cierre | `mart_kpis_geo` | Gerencia de Ventas |
| Ingreso por comisión | `mart_kpis_geo` | Gerencia de Ventas |
| Precio por m² | `mart_kpis_geo` | Marketing |
| Evolución mensual | `mart_kpis_tiempo` | Marketing |
| Tasa de cierre | `mart_kpis_geo` | Marketing |
