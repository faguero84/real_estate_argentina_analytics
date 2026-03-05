
  
  create view "real_estate"."main_staging"."stg_properties__dbt_tmp" as (
    -- models/staging/stg_properties.sql
-- ===================================
-- Modelo STAGING: limpieza y tipado de la tabla cruda.
--
-- Responsabilidades de staging:
--   ✓ Renombrar columnas a nombres claros
--   ✓ Castear tipos de datos correctos
--   ✓ Filtrar filas inválidas (precio nulo, fechas inválidas)
--   ✓ Normalizar strings (trim, uppercase)
--   ✗ NO hace joins
--   ✗ NO agrega lógica de negocio
--   ✗ NO crea métricas

WITH source AS (
    -- Referenciamos la tabla RAW que cargó load_raw.py
    SELECT * FROM raw.properties
),

cleaned AS (
    SELECT
        -- ── Operación ─────────────────────────────────────────
        TRIM(operation_type)                        AS operation_type,
        TRIM(currency)                              AS currency,

        -- ── Propiedad ─────────────────────────────────────────
        TRIM(property_type)                         AS property_type,
        TRY_CAST(rooms AS INTEGER)                  AS rooms,
        -- TRY_CAST devuelve NULL en vez de error si el valor es inválido
        -- Más robusto que CAST para datos sucios

        -- ── Geografía ─────────────────────────────────────────
        TRIM(l1)                                    AS pais,
        TRIM(l2)                                    AS provincia,
        TRIM(l3)                                    AS ciudad,

        -- ── Fechas ────────────────────────────────────────────
        TRY_CAST(start_date AS DATE)                AS fecha_alta,
        TRY_CAST(end_date   AS DATE)                AS fecha_baja,

        -- ── Métricas base ─────────────────────────────────────
        TRY_CAST(price           AS DOUBLE)         AS precio,
        TRY_CAST(surface_covered AS DOUBLE)         AS superficie_cubierta,
        TRY_CAST(surface_total   AS DOUBLE)         AS superficie_total,

        -- ── Metadatos de carga ────────────────────────────────
        _loaded_at,
        _source_file

    FROM source
),

validated AS (
    SELECT *
    FROM cleaned
    WHERE
        -- Filtros de calidad — registros que no tienen sentido analítico
        operation_type IN ('Venta', 'Alquiler', 'Alquiler temporal')
        AND currency       IN ('ARS', 'USD')
        AND precio         IS NOT NULL
        AND precio         > 0
        AND fecha_alta     IS NOT NULL
        AND ciudad         IS NOT NULL
        AND provincia      IS NOT NULL
        -- Descartamos superficies imposibles (negativas o gigantes)
        AND (superficie_total   IS NULL OR superficie_total   BETWEEN 5 AND 50000)
        AND (superficie_cubierta IS NULL OR superficie_cubierta BETWEEN 5 AND 50000)
)

SELECT * FROM validated
  );
