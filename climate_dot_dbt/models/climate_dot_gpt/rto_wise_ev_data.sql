{{
    config(
        materialized='incremental',
        unique_key=['date', 'rto_code', 'vehicle_type', 'vehicle_category', 'vehicle_use_type', 'vehicle_class']
    )
}}

SELECT
    CAST(year AS INT) as year,
    CAST(month AS INT) as month,
    CAST(day AS INT) as day,
    CAST(date AS DATE) as date,
    CAST(state AS VARCHAR(100)) as state,
    CAST(d.district AS VARCHAR(100)) as district,
    CAST(rto_name AS VARCHAR(100)) as rto_name,
    CAST(f.rto_code AS VARCHAR(50)) as rto_code,
    CAST(vehicle_type AS VARCHAR(100)) as vehicle_type,
    CAST(vehicle_category AS VARCHAR(100)) as vehicle_category,
    CAST(vehicle_use_type AS VARCHAR(100)) as vehicle_use_type,
    CAST(vehicle_class AS VARCHAR(100)) as vehicle_class,
    CAST(REPLACE(cng_only, ',', '') AS INT) as cng_only,
    CAST(REPLACE(diesel, ',', '') AS INT) as diesel,
    CAST(REPLACE(diesel_hybrid, ',', '') AS INT) as diesel_hybrid,
    CAST(REPLACE(di_methyl_ether, ',', '') AS INT) as di_methyl_ether,
    CAST(REPLACE(dual_diesel_bio_cng, ',', '') AS INT) as dual_diesel_bio_cng,
    CAST(REPLACE(dual_diesel_cng, ',', '') AS INT) as dual_diesel_cng,
    CAST(REPLACE(dual_diesel_lng, ',', '') AS INT) as dual_diesel_lng,
    CAST(REPLACE(electric_vehicles, ',', '') AS INT) as electric_vehicles,
    CAST(REPLACE(ethanol, ',', '') AS INT) as ethanol,
    CAST(REPLACE(lng, ',', '') AS INT) as lng,
    CAST(REPLACE(lpg_only, ',', '') AS INT) as lpg_only,
    CAST(REPLACE(methanol, ',', '') AS INT) as methanol,
    CAST(REPLACE(not_applicable, ',', '') AS INT) as not_applicable,
    CAST(REPLACE(petrol, ',', '') AS INT) as petrol,
    CAST(REPLACE(petrol_cng, ',', '') AS INT) as petrol_cng,
    CAST(REPLACE(petrol_ethanol, ',', '') AS INT) as petrol_ethanol,
    CAST(REPLACE(petrol_hybrid, ',', '') AS INT) as petrol_hybrid,
    CAST(REPLACE(petrol_lpg, ',', '') AS INT) as petrol_lpg,
    CAST(REPLACE(petrol_methanol, ',', '') AS INT) as petrol_methanol,
    CAST(REPLACE(plug_in_hybrid_ev, ',', '') AS INT) as plug_in_hybrid_ev,
    CAST(REPLACE(pure_ev, ',', '') AS INT) as pure_ev,
    CAST(REPLACE(solar, ',', '') AS INT) as solar,
    CAST(REPLACE(strong_hybrid_ev, ',', '') AS INT) as strong_hybrid_ev,
    CAST(REPLACE(total, ',', '') AS INT) as total,
    CAST(inserted_at AS DATETIME) as inserted_at
FROM {{ source('raw_data', 'fact_ev_data_by_rto') }} f
LEFT JOIN {{ source('raw_data', 'rto_code_to_district_mapping') }} d
    ON f.rto_code = d.rto_code
{% if is_incremental() %}
WHERE date >= DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
{% endif %} 