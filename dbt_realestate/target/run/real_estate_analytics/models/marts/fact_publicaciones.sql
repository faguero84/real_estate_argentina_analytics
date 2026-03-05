
  
    
    

    create  table
      "real_estate"."main_marts"."fact_publicaciones__dbt_tmp"
  
    as (
      -- models/marts/fact_publicaciones.sql
-- =====================================
-- Modelo MART: tabla de hechos lista para Tableau.
--
-- Este modelo construye la tabla central del star schema
-- con todas las métricas calculadas y claves foráneas resueltas.
-- Es el modelo que Tableau va a consumir directamente.

WITH base AS (
    -- Referenciamos el modelo de staging con ref()
    -- ref() es la forma de dbt de conectar modelos entre sí
    -- Ventaja: dbt construye automáticamente el orden de ejecución
    SELECT * FROM "real_estate"."main_staging"."stg_properties"
),

with_metrics AS (
    SELECT
        -- ── Identificador único ───────────────────────────────
        ROW_NUMBER() OVER (ORDER BY fecha_alta, operation_type) AS id_publicacion,

        -- ── Dimensiones (para filtros en Tableau) ────────────
        operation_type,
        currency,
        property_type,
        rooms,
        pais,
        provincia,
        ciudad,

        -- ── Dimensión tiempo descompuesta ────────────────────
        -- Tableau puede hacer esto, pero tenerlo precalculado
        -- hace los dashboards más rápidos
        fecha_alta,
        fecha_baja,
        YEAR(fecha_alta)                            AS anio,
        QUARTER(fecha_alta)                         AS trimestre,
        MONTH(fecha_alta)                           AS mes,
        DAYOFWEEK(fecha_alta)                       AS dia_semana,
        -- Nombre del mes en español para labels en Tableau
        CASE MONTH(fecha_alta)
            WHEN 1  THEN 'Enero'    WHEN 2  THEN 'Febrero'
            WHEN 3  THEN 'Marzo'    WHEN 4  THEN 'Abril'
            WHEN 5  THEN 'Mayo'     WHEN 6  THEN 'Junio'
            WHEN 7  THEN 'Julio'    WHEN 8  THEN 'Agosto'
            WHEN 9  THEN 'Septiembre' WHEN 10 THEN 'Octubre'
            WHEN 11 THEN 'Noviembre'  WHEN 12 THEN 'Diciembre'
        END                                         AS nombre_mes,

        -- ── Métricas de precio ────────────────────────────────
        precio,
        CASE
            WHEN superficie_total   > 0 THEN ROUND(precio / superficie_total,   2)
            WHEN superficie_cubierta > 0 THEN ROUND(precio / superficie_cubierta, 2)
        END                                         AS precio_por_m2,

        -- ── Métricas de superficie ────────────────────────────
        superficie_cubierta,
        superficie_total,

        -- ── Métricas de comisión ─────────────────────────────
        -- La comisión varía por tipo de operación (lógica de negocio inmobiliario)
        CASE operation_type
            WHEN 'Venta'             THEN 0.03
            WHEN 'Alquiler'          THEN 0.10
            WHEN 'Alquiler temporal' THEN 0.15
        END                                         AS porcentaje_comision,
        ROUND(
            precio * CASE operation_type
                WHEN 'Venta'             THEN 0.03
                WHEN 'Alquiler'          THEN 0.10
                WHEN 'Alquiler temporal' THEN 0.15
            END, 2
        )                                           AS ingreso_comision,

        -- ── Métrica de tiempo de cierre ───────────────────────
        -- CORRECCIÓN vs modelo 2021: calculado aquí como medida,
        -- no almacenado como par de FKs sin utilidad analítica
        CASE
            WHEN fecha_baja IS NOT NULL AND fecha_baja >= fecha_alta
            THEN DATE_DIFF('day', fecha_alta, fecha_baja)
        END                                         AS dias_publicacion,

        -- ── Flag operación cerrada ────────────────────────────
        CASE WHEN fecha_baja IS NOT NULL THEN 1 ELSE 0
        END                                         AS es_operacion_cerrada,

        -- ── Metadatos ─────────────────────────────────────────
        _loaded_at

    FROM base
)

SELECT * FROM with_metrics
    );
  
  