-- models/marts/mart_kpis_geo.sql
-- ================================
-- KPIs geográficos agregados para el dashboard de Tableau.
--
-- Este modelo responde directamente los requerimientos
-- de la gerencia de Ventas y Marketing del trabajo práctico:
--   → Zonas geográficas con mayor ganancia por tipo de operación
--   → Tiempo de cierre por zona
--   → Precio por m² por provincia y ciudad

SELECT
    provincia,
    ciudad,
    operation_type,
    currency,

    -- Volumen
    COUNT(*)                                    AS cant_publicaciones,
    SUM(es_operacion_cerrada)                   AS cant_cerradas,
    ROUND(
        SUM(es_operacion_cerrada) * 100.0 / COUNT(*), 1
    )                                           AS tasa_cierre_pct,

    -- Precios
    ROUND(AVG(precio), 2)                       AS precio_promedio,
    ROUND(MEDIAN(precio), 2)                    AS precio_mediana,
    ROUND(AVG(precio_por_m2), 2)                AS precio_m2_promedio,

    -- Comisiones
    ROUND(SUM(ingreso_comision), 2)             AS total_comision,
    ROUND(AVG(ingreso_comision), 2)             AS comision_promedio,

    -- Tiempo de cierre
    ROUND(AVG(dias_publicacion), 1)             AS promedio_dias_cierre,
    ROUND(MEDIAN(dias_publicacion), 1)          AS mediana_dias_cierre,

    -- Superficie
    ROUND(AVG(superficie_cubierta), 1)          AS sup_cubierta_promedio,
    ROUND(AVG(superficie_total), 1)             AS sup_total_promedio

FROM "real_estate"."main_marts"."fact_publicaciones"
GROUP BY provincia, ciudad, operation_type, currency