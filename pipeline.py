"""
prefect/pipeline.py
====================
Orquestación completa del pipeline con Prefect 3.

Concepto clave — ¿qué es Prefect?
  Es el "director de orquesta" del pipeline. No transforma datos —
  coordina quién hace qué y en qué orden. Si algo falla, Prefect
  registra el error, reintenta automáticamente, y te avisa.

  En términos de portfolio: muestra que sabés construir pipelines
  productivos, no solo scripts que se ejecutan una vez.

Estructura de este archivo:
  @task   → unidad mínima de trabajo (una función Python)
  @flow   → conjunto de tasks con dependencias entre sí
  
Ejecución:
  python prefect/pipeline.py              ← correr una vez
  prefect deploy prefect/pipeline.py      ← programar en Prefect Cloud
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

PROJECT_DIR  = Path(os.getenv("PROJECT_DIR",  "."))
DBT_DIR      = PROJECT_DIR / "dbt_realestate"
DUCKDB_PATH  = os.getenv("DUCKDB_PATH", "./data/real_estate.duckdb")


# ════════════════════════════════════════════════════════════
# TASKS
# Cada @task es una unidad atómica de trabajo.
# Prefect trackea su estado: PENDING → RUNNING → COMPLETED/FAILED
# ════════════════════════════════════════════════════════════

@task(
    name="Descargar dataset de Kaggle",
    retries=2,                          # reintenta 2 veces si falla la red
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,       # si el input no cambió, no re-descarga
    cache_expiration=timedelta(hours=24)
)
def task_download_kaggle() -> str:
    """Descarga el dataset desde Kaggle y retorna la ruta del CSV."""
    logger = get_run_logger()
    logger.info("📥 Iniciando descarga desde Kaggle...")
    
    import kagglehub
    path = kagglehub.dataset_download("msorondo/argentina-venta-de-propiedades")
    
    csv_candidates = list(Path(path).glob("*.csv"))
    if not csv_candidates:
        raise FileNotFoundError(f"No se encontró CSV en {path}")
    
    csv_path = str(csv_candidates[0])
    size_mb  = Path(csv_path).stat().st_size / 1024**2
    logger.info(f"✅ Dataset disponible: {csv_path} ({size_mb:.1f} MB)")
    return csv_path


@task(name="Cargar datos crudos en DuckDB")
def task_load_raw(csv_path: str) -> int:
    """Carga el CSV en la capa raw de DuckDB. Retorna cantidad de filas."""
    logger = get_run_logger()
    logger.info(f"🦆 Cargando en DuckDB: {DUCKDB_PATH}")
    
    import duckdb
    
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.properties AS
        SELECT *, current_timestamp AS _loaded_at, '{Path(csv_path).name}' AS _source_file
        FROM read_csv_auto('{csv_path}', all_varchar = true)
    """)
    
    row_count = con.execute("SELECT COUNT(*) FROM raw.properties").fetchone()[0]
    con.close()
    
    logger.info(f"✅ raw.properties: {row_count:,} filas cargadas")
    return row_count


@task(name="Ejecutar modelos dbt")
def task_dbt_run() -> bool:
    """
    Ejecuta dbt run — construye todos los modelos SQL.
    
    Esto compila y ejecuta los modelos en este orden:
      1. staging.stg_properties   (vista)
      2. marts.fact_publicaciones (tabla)
      3. marts.mart_kpis_geo      (tabla)
      4. marts.mart_kpis_tiempo   (tabla)
    """
    logger = get_run_logger()
    logger.info("⚙️  Ejecutando dbt run...")
    
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd=DBT_DIR,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.error(f"❌ dbt run falló:\n{result.stderr}")
        raise RuntimeError(f"dbt run failed: {result.stderr}")
    
    logger.info(f"✅ dbt run completado:\n{result.stdout}")
    return True


@task(name="Ejecutar tests de dbt")
def task_dbt_test() -> bool:
    """
    Ejecuta dbt test — valida la calidad de los datos.
    
    Los tests definidos en schema.yml se convierten en queries SQL
    que verifican: not_null, unique, accepted_values.
    Si un test falla, el pipeline se detiene y Prefect registra el error.
    """
    logger = get_run_logger()
    logger.info("🧪 Ejecutando dbt test...")
    
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd=DBT_DIR,
        capture_output=True,
        text=True
    )
    
    # Mostrar resumen de tests
    for line in result.stdout.split('\n'):
        if any(k in line for k in ['PASS', 'FAIL', 'ERROR', 'Finished']):
            logger.info(line)
    
    if result.returncode != 0:
        logger.error(f"❌ Tests fallidos:\n{result.stdout}")
        raise RuntimeError("dbt tests failed — revisar calidad de datos")
    
    logger.info("✅ Todos los tests pasaron")
    return True


@task(name="Exportar CSVs para Tableau")
def task_export_for_tableau() -> list:
    """
    Exporta los marts como CSV para conectar con Tableau Desktop.
    
    Tableau Desktop puede conectarse directamente a DuckDB,
    pero los CSVs son la opción más universal (funciona con
    Tableau Public también).
    """
    logger = get_run_logger()
    logger.info("📊 Exportando datos para Tableau...")
    
    import duckdb
    
    output_dir = PROJECT_DIR / "data" / "tableau"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(DUCKDB_PATH)
    
    exports = [
        ("marts.fact_publicaciones", "fact_publicaciones.csv"),
        ("marts.mart_kpis_geo",      "kpis_geograficos.csv"),
        ("marts.mart_kpis_tiempo",   "kpis_temporales.csv"),
    ]
    
    exported_files = []
    for table, filename in exports:
        filepath = output_dir / filename
        con.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{filepath}'
            (HEADER, DELIMITER ',')
        """)
        rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"  ✅ {filename}: {rows:,} filas")
        exported_files.append(str(filepath))
    
    con.close()
    logger.info(f"📁 Archivos exportados en: {output_dir}")
    return exported_files


# ════════════════════════════════════════════════════════════
# FLOW
# El @flow conecta las tasks y define el orden de ejecución.
# Prefect maneja automáticamente el DAG (grafo de dependencias).
# ════════════════════════════════════════════════════════════

@flow(
    name="real-estate-argentina-pipeline",
    description="Pipeline completo Real Estate Argentina: Kaggle → DuckDB → dbt → Tableau",
    log_prints=True
)
def real_estate_pipeline():
    """
    Pipeline principal del Data Warehouse Real Estate Argentina Analytics.
    
    Flujo:
      Kaggle API → raw.properties → dbt models → CSVs para Tableau
    
    Para ver los runs en la UI de Prefect:
      1. prefect cloud login
      2. Cada vez que corre este flow, aparece en el dashboard
    """
    logger = get_run_logger()
    logger.info("🚀 Iniciando pipeline Real Estate Argentina Analytics")
    logger.info(f"   Timestamp: {datetime.now().isoformat()}")
    
    # Step 1: Extraer datos de Kaggle
    csv_path = task_download_kaggle()
    
    # Step 2: Cargar en DuckDB (depende del step 1)
    row_count = task_load_raw(csv_path)
    
    # Step 3: Transformar con dbt (depende del step 2)
    dbt_ok = task_dbt_run(wait_for=[row_count])
    
    # Step 4: Validar calidad (depende del step 3)
    tests_ok = task_dbt_test(wait_for=[dbt_ok])
    
    # Step 5: Exportar para Tableau (depende del step 4)
    exports = task_export_for_tableau(wait_for=[tests_ok])
    
    logger.info(f"✅ Pipeline completado. Archivos Tableau: {exports}")
    return exports


# ════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Ejecutar localmente:
    #   python prefect/pipeline.py
    #
    # Para ver en Prefect Cloud:
    #   1. pip install prefect
    #   2. prefect cloud login   (gratis, requiere cuenta en prefect.io)
    #   3. python prefect/pipeline.py
    #   → el run aparece en https://app.prefect.cloud
    
    real_estate_pipeline()
