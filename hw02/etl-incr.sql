BEGIN;


INSERT INTO public.dim_date (day, month, year, quarter, day_of_week, date)
SELECT DISTINCT
    EXTRACT(DAY FROM tu.time)::integer,
    EXTRACT(MONTH FROM tu.time)::integer,
    EXTRACT(YEAR FROM tu.time)::integer,
    EXTRACT(QUARTER FROM tu.time)::integer,
    EXTRACT(ISODOW FROM tu.time)::integer,
    tu.time::date as date
FROM staging.import_tracking_upd tu
WHERE NOT EXISTS (
    SELECT 1 FROM public.dim_date d WHERE d.date = tu.time::date
);

-- ------------------

UPDATE public.dim_car d
SET 
    valid_to = '2025-04-01 00:00:00'::timestamp,
    current_row = 'inactive'
FROM staging.car_info_upd cu
WHERE d.car_key = cu.car_key
  AND d.current_row = 'active'
  AND (
       d.company_key   IS DISTINCT FROM cu.company_key OR
       d.license_plate IS DISTINCT FROM COALESCE(NULLIF(cu.license_plate, ''), 'UNKNOWN') OR
       d.make          IS DISTINCT FROM COALESCE(NULLIF(cu.make, ''), 'UNKNOWN') OR
       d.color         IS DISTINCT FROM COALESCE(NULLIF(cu.color, ''), 'UNKNOWN') OR
       d.tonnage       IS DISTINCT FROM cu.tonnage::real OR
       d.type          IS DISTINCT FROM COALESCE(NULLIF(cu.type, ''), 'UNKNOWN')
  );

-- ------------------

INSERT INTO public.dim_car (
    car_key, company_key, license_plate, make, color, tonnage, type, 
    valid_from, valid_to, current_row
)
SELECT
    cu.car_key,
    cu.company_key,
    COALESCE(NULLIF(cu.license_plate, ''), 'UNKNOWN') AS license_plate,
    COALESCE(NULLIF(cu.make, ''), 'UNKNOWN') AS make,
    COALESCE(NULLIF(cu.color, ''), 'UNKNOWN') AS color,
    cu.tonnage::real,
    COALESCE(NULLIF(cu.type, ''), 'UNKNOWN') AS type,
    '2025-04-01 00:00:00'::timestamp,
    'infinity'::timestamp,
    'active'
FROM staging.car_info_upd cu
WHERE NOT EXISTS (
    SELECT 1 FROM public.dim_car d 
    WHERE d.car_key = cu.car_key AND d.current_row = 'active'
);

-- ------------------

INSERT INTO public.fact_tracking (
    company_id, car_id, driver_id, time_id, date_id, 
    pos_key, "time", truck_status, pos_gps, speed, distance, driving_time
)
SELECT DISTINCT ON (dcp.company_id, dca.car_id, ddr.driver_id, dti.time_id, dda.date_id)
    dcp.company_id,
    dca.car_id,
    ddr.driver_id,
    dti.time_id,
    dda.date_id,
    itu.pos_key,
    itu.time,
    itu.truck_status,
    itu.pos_gps,
    itu.speed::numeric,
    itu.distance::numeric,
    itu.driving_time::integer
FROM staging.import_tracking_upd itu

JOIN public.dim_date dda
    ON dda.date = itu.time::date

JOIN public.dim_car dca
    ON dca.car_key = itu.car_key
    AND itu.time >= dca.valid_from
    AND itu.time < dca.valid_to

JOIN public.dim_company dcp
    ON dcp.company_key = dca.company_key

JOIN public.dim_driver ddr
    ON ddr.driver_key = itu.driver_key

JOIN public.dim_time dti
    ON EXTRACT(HOUR FROM itu.time) = dti.hour
    AND EXTRACT(MINUTE FROM itu.time) = dti.minute
    AND EXTRACT(SECOND FROM itu.time) = dti.second

WHERE NOT EXISTS (
    SELECT 1 FROM public.fact_tracking ftr
    WHERE ftr.company_id = dcp.company_id
      AND ftr.car_id     = dca.car_id
      AND ftr.driver_id  = ddr.driver_id
      AND ftr.time_id    = dti.time_id
      AND ftr.date_id    = dda.date_id
)
ORDER BY
    dcp.company_id,
    dca.car_id,
    ddr.driver_id,
    dti.time_id,
    dda.date_id,
    itu.pos_key DESC;



COMMIT;
