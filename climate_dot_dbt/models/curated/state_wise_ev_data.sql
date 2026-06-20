{{
    config(
        materialized='incremental',
        unique_key=['date', 'state', 'vehicle_type', 'vehicle_category', 'vehicle_use_type', 'vehicle_class']
    )
}}

SELECT
    CAST(year AS INT) as year,
    CAST(month AS INT) as month,
    CAST(day AS INT) as day,
    CONVERT(DATE, [date], 103) as date,
    CAST(state AS VARCHAR(100)) as state,
    CAST(vehicle_type AS VARCHAR(100)) as vehicle_type,
    CAST(vehicle_category AS VARCHAR(100)) as vehicle_category,
    CAST(vehicle_use_type AS VARCHAR(100)) as vehicle_use_type,
    CAST(vehicle_class AS VARCHAR(100)) as vehicle_class,
    CAST(REPLACE(bio_cng_bio_gas, ',', '') AS INT) as bio_cng_bio_gas,
    CAST(REPLACE(bio_diesel_b100, ',', '') AS INT) as bio_diesel_b100,
    CAST(REPLACE(bio_methane, ',', '') AS INT) as bio_methane,
    CAST(REPLACE(cng_only, ',', '') AS INT) as cng_only,
    CAST(REPLACE(diesel, ',', '') AS INT) as diesel,
    CAST(REPLACE(diesel_hybrid, ',', '') AS INT) as diesel_hybrid,
    CAST(REPLACE(di_methyl_ether, ',', '') AS INT) as di_methyl_ether,
    CAST(REPLACE(dual_diesel_bio_cng, ',', '') AS INT) as dual_diesel_bio_cng,
    CAST(REPLACE(dual_diesel_cng, ',', '') AS INT) as dual_diesel_cng,
    CAST(REPLACE(dual_diesel_lng, ',', '') AS INT) as dual_diesel_lng,
    CAST(REPLACE(electric_bov, ',', '') AS INT) as electric_bov,
    CAST(REPLACE(ethanol_e100, ',', '') AS INT) as ethanol_e100,
    CAST(REPLACE(flex_fuel_bio_diesel, ',', '') AS INT) as flex_fuel_bio_diesel,
    CAST(REPLACE(flex_fuel_ethanol, ',', '') AS INT) as flex_fuel_ethanol,
    CAST(REPLACE(fuel_cell_hydrogen, ',', '') AS INT) as fuel_cell_hydrogen,
    CAST(REPLACE(hcng, ',', '') AS INT) as hcng,
    CAST(REPLACE(hydrogen_ice, ',', '') AS INT) as hydrogen_ice,
    CAST(REPLACE(lng, ',', '') AS INT) as lng,
    CAST(REPLACE(lpg_only, ',', '') AS INT) as lpg_only,
    CAST(REPLACE(methanol, ',', '') AS INT) as methanol,
    CAST(REPLACE(not_applicable, ',', '') AS INT) as not_applicable,
    CAST(REPLACE(petrol, ',', '') AS INT) as petrol,
    CAST(REPLACE(petrol_cng, ',', '') AS INT) as petrol_cng,
    CAST(REPLACE(petrol_e20, ',', '') AS INT) as petrol_e20,
    CAST(REPLACE(petrol_e20_cng, ',', '') AS INT) as petrol_e20_cng,
    CAST(REPLACE(petrol_e20_hybrid, ',', '') AS INT) as petrol_e20_hybrid,
    CAST(REPLACE(petrol_e20_hybrid_cng, ',', '') AS INT) as petrol_e20_hybrid_cng,
    CAST(REPLACE(petrol_e20_lpg, ',', '') AS INT) as petrol_e20_lpg,
    CAST(REPLACE(petrol_hybrid, ',', '') AS INT) as petrol_hybrid,
    CAST(REPLACE(petrol_hybrid_cng, ',', '') AS INT) as petrol_hybrid_cng,
    CAST(REPLACE(petrol_lpg, ',', '') AS INT) as petrol_lpg,
    CAST(REPLACE(petrol_methanol, ',', '') AS INT) as petrol_methanol,
    CAST(REPLACE(plug_in_hybrid_ev, ',', '') AS INT) as plug_in_hybrid_ev,
    CAST(REPLACE(pure_ev, ',', '') AS INT) as pure_ev,
    CAST(REPLACE(solar, ',', '') AS INT) as solar,
    CAST(REPLACE(strong_hybrid_ev, ',', '') AS INT) as strong_hybrid_ev,
    CAST(REPLACE(total, ',', '') AS INT) as total,
    CAST(inserted_at AS DATETIME) as inserted_at
FROM {{ source('raw_data', 'fact_ev_data_by_state') }}
{% if is_incremental() %}
WHERE CONVERT(DATE, [date], 103) >= DATEADD(MONTH, -1, DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1))
{% endif %}
