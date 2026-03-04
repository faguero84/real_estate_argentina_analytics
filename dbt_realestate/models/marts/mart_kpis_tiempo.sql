-- models/marts/mart_kpis_tiempo.sql
-- ====================================
-- Serie temporal mensual para el dashboard de Tableau.
-- Responde: evolución de publicaciones, comisiones e ingresos por mes.

SELECT
    anio,
    mes,
    nombre_mes,
    -- Columna auxiliar para ordenar correctamente en Tableau
    -- sin depender del nombre del mes (que es string)
    MAKE_DATE(anio, mes, 1)                     AS periodo,
    operation_type,
    currency,

    COUNT(*)                                    AS cant_publicaciones,
    SUM(es_operacion_cerrada)                   AS cant_cerradas,
    ROUND(SUM(ingreso_comision), 2)             AS total_comision,
    ROUND(AVG(precio), 2)                       AS precio_promedio,
    ROUND(AVG(dias_publicacion), 1)             AS promedio_dias_cierre

FROM {{ ref('fact_publicaciones') }}
GROUP BY anio, mes, nombre_mes, operation_type, currency
ORDER BY anio, mes
