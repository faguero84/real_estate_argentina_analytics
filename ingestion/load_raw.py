"""
ingestion/load_raw.py
=====================
Paso 1 del pipeline: descarga el dataset de Kaggle y carga
la capa RAW en DuckDB como tabla sin transformar.

Concepto clave — capas del DW:
  RAW     → datos tal cual vienen de la fuente (sin tocar)
  STAGING → limpieza y tipado (dbt se encarga)
  MARTS   → modelos analíticos finales (dbt se encarga)

Este script solo maneja RAW. Las transformaciones
son responsabilidad de dbt, no del script de ingesta.
"""

import os
import duckdb
import kagglehub
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Configuración ────────────────────────────────────────────
DUCKDB_PATH  = os.getenv("DUCKDB_PATH", "./data/real_estate.duckdb")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./data/raw")
KAGGLE_DATASET = "msorondo/argentina-venta-de-propiedades"


def download_dataset() -> Path:
    """
    Descarga el dataset desde Kaggle usando la API.
    Si ya fue descargado antes, usa el caché local.

    Requiere que kaggle.json esté configurado, o las variables
    de entorno KAGGLE_USERNAME y KAGGLE_KEY en el .env
    """
    print("📥 Descargando dataset desde Kaggle...")
    
    # kagglehub usa automáticamente las variables de entorno
    # KAGGLE_USERNAME y KAGGLE_KEY si kaggle.json no existe
    path = kagglehub.dataset_download(KAGGLE_DATASET)
    csv_path = Path(path) / "ar_properties.csv"
    
    if not csv_path.exists():
        # Buscar cualquier CSV en la carpeta descargada
        csvs = list(Path(path).glob("*.csv"))
        if not csvs:
            raise FileNotFoundError(f"No se encontró CSV en {path}")
        csv_path = csvs[0]
    
    size_mb = csv_path.stat().st_size / 1024**2
    print(f"   ✅ Dataset listo: {csv_path.name} ({size_mb:.1f} MB)")
    return csv_path


def load_raw_to_duckdb(csv_path: Path) -> dict:
    """
    Carga el CSV crudo en DuckDB como tabla raw.properties.

    Decisión de diseño: usamos DuckDB para leer el CSV directamente
    con SQL, sin pasar por Pandas. Esto es más eficiente para 
    archivos grandes (2M filas) y muestra el poder de DuckDB.
    """
    print(f"\n🦆 Conectando a DuckDB: {DUCKDB_PATH}")
    
    # Crear carpeta si no existe
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(DUCKDB_PATH)
    
    # Crear schema RAW (es como una "carpeta" dentro del warehouse)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    print(f"   📂 Leyendo CSV con DuckDB SQL...")
    
    # DuckDB puede leer el CSV directamente con SQL — no necesita Pandas
    # Esto es una de las características más potentes de DuckDB
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.properties AS
        SELECT
            *,
            -- Metadatos de carga (buena práctica: siempre registrar cuándo entró el dato)
            current_timestamp AS _loaded_at,
            '{csv_path.name}'  AS _source_file
        FROM read_csv_auto(
            '{csv_path}',
            all_varchar = true,
            ignore_errors = true,
            null_padding = true
        )
    """)
    
    # Verificación post-carga
    stats = con.execute("""
        SELECT 
            COUNT(*)                                    AS total_filas,
            COUNT(DISTINCT operation_type)              AS tipos_operacion,
            COUNT(DISTINCT l2)                          AS provincias,
            COUNT(DISTINCT l3)                          AS ciudades,
            SUM(CASE WHEN price IS NULL THEN 1 END)     AS nulos_precio,
            MIN(start_date)                             AS fecha_min,
            MAX(start_date)                             AS fecha_max
        FROM raw.properties
    """).fetchdf()
    
    print("\n   📊 Resumen de la carga RAW:")
    print(f"      Total filas       : {stats['total_filas'][0]:>12,}")
    print(f"      Tipos de operación: {stats['tipos_operacion'][0]:>12}")
    print(f"      Provincias        : {stats['provincias'][0]:>12}")
    print(f"      Ciudades          : {stats['ciudades'][0]:>12}")
    print(f"      Nulos en precio   : {stats['nulos_precio'][0]:>12,}")
    print(f"      Rango de fechas   : {stats['fecha_min'][0]} → {stats['fecha_max'][0]}")
    
    # Guardar metadatos de la carga (útil para debugging y auditoría)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw._load_history (
            loaded_at     TIMESTAMP,
            source_file   VARCHAR,
            total_rows    INTEGER,
            status        VARCHAR
        )
    """)
    
    con.execute(f"""
        INSERT INTO raw._load_history VALUES (
            current_timestamp,
            '{csv_path.name}',
            (SELECT COUNT(*) FROM raw.properties),
            'SUCCESS'
        )
    """)
    
    con.close()
    print(f"\n   ✅ raw.properties cargada en {DUCKDB_PATH}")
    
    return stats.to_dict('records')[0]


def main():
    print("=" * 55)
    print("  Real Estate Argentina Analytics — Ingesta RAW")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)
    
    csv_path = download_dataset()
    stats    = load_raw_to_duckdb(csv_path)
    
    print("\n✅ Ingesta completada. Próximo paso: ejecutar dbt")
    print("   $ cd dbt_realestate && dbt run")


if __name__ == "__main__":
    main()
